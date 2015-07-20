package Backup::Glacier::Journal;

=head1 NAME

    Backup::Glacier::Journal - module for logging glacier activity, journal
                               serves also as source of info for App::MtAws
                               glacier library, it overrides original App::MtAws
                               library and implements SQLite as journal not file

=head1 SYNOPSIS

    my $j = Backup::Glacier::Journal->new(
                                      %journal_opts, 
                                      'journal_file' => $config->{'journal'}, 
                                      'root_dir' => $bkpDir
                                  );
                                  
    $j->read_journal(should_exist => 0);
    $j->read_files($read_journal_opts, $config->{'max-number-of-files'});

=cut

use base qw/App::MtAws::Journal/;

use Carp;
use Try::Tiny;
use warnings;
use autodie;
use File::Glob;
use File::Copy;
use File::Path;
use File::Basename;
use IO::File;
use DateTime;
use Data::Dumper;
use DBI;

use App::MtAws::Utils;

=head1 METHODS

=over 12

=item C<new>

Constructor overrides original

param:

    %params - as superclass App::MtAws::Journal
    
return:

    $self object of type Backup::Glacier::Journal

=cut

sub new {

    my $class = shift;
    my %params = @_;
    $debugLogger = $params{'debugLogger'};
    $infoLogger = $params{'infoLogger'};
    delete $params{'debugLogger'};
    delete $params{'infoLogger'};
    
    my $self = $class->SUPER::new(%params);
    $self->{'debugLogger'} = $debugLogger;
    $self->{'infoLogger'} = $infoLogger;
    
    $self->{'dbh'} = DBI->connect(
                    "dbi:SQLite:dbname=" . $self->{'journal_file'},
                    "", 
                    "",
                    {'RaiseError' => 1}
                );
                
    return $self;
    
} # end sub new

=item C<read_journal>

Read entries from journal and sort them as archive or job

param:

    %params - as superclass App::MtAws::Journal
    
return:

    true boolean
    
=cut

sub read_journal {

    my ($self, %args) = @_;
    confess unless defined $args{'should_exist'};
    confess unless length($self->{'journal_file'});
    $self->{'last_read_time'} = time();
    $self->{'active_retrievals'} = {} if $self->{'use_active_retrievals'};

    my $binary_filename = binaryfilename $self->{'journal_file'};

    if ($args{should_exist} && !-e $binary_filename) {
        confess;
    } elsif (-e $binary_filename) {

        my @glcBkpsPresent = ();
        my @glcRetrieveJobs = ();

        $self->{'debugLogger'}->debug("Reading journal not delete entries from database");
        
        my $deletedQuery = "SELECT journal.glacier_id FROM journal";
        $deletedQuery .= " WHERE type='DELETED'";

        my $query = "SELECT * FROM journal JOIN glacier ON";
        $query .= " journal.glacier_id = glacier.glacier_id WHERE";
        $query .= " journal.glacier_id NOT IN (" . $deletedQuery . ")";
        $query .= " AND type='CREATED' ORDER BY mtime ASC";

        $self->{'debugLogger'}->debug('Query: ', $query);
        
        try {
            @glcBkpsPresent = @{ $self->{'dbh'}->selectall_arrayref($query, { Slice => {} }) };
        } catch {
            my $error = @_ || $_;
            $self->{'infoLogger'}->error("Error: ", $error);
            croak "Error: " . $error;
        };

        $self->{'debugLogger'}->debug("Dumping all notdelete journal entries", sub { Dumper(\@glcBkpsPresent)});
        
        $self->{'archive_sorted'} = \@glcBkpsPresent;

        for my $glcBkpPresent(@glcBkpsPresent) {

            $self->_add_archive({
                    'relfilename' => $glcBkpPresent->{'relfilename'},
                    'time' => $glcBkpPresent->{'time'}, # numify
                    'archive_id' => $glcBkpPresent->{'archive_id'},
                    'size' => $glcBkpPresent->{'size'}, # numify
                    'mtime' => defined($glcBkpPresent->{'mtime'}) ? $glcBkpPresent->{'mtime'} : undef,
                    'treehash' => $glcBkpPresent->{'treehash'},
            });

        } # for

        $self->{'debugLogger'}->debug("Reading journal retrieve entries from database");
        
        my $retrieveQuery = "SELECT * FROM journal WHERE type='RETRIEVE_JOB'";

        try {
            @glcRetrieveJobs = @{ $self->{'dbh'}->selectall_arrayref($retrieveQuery, { Slice => {} }) };
        } catch {
            my $error = @_ || $_;
            $self->{'infoLogger'}->error("Error: ", $error);
            croak "Error: " . $error;
        };

        $self->{'debugLogger'}->debug("Dumping retrieve entries", sub { Dumper(\@glcRetrieveJobs)});
        
        for my $job(@glcRetrieveJobs) {

            $self->_retrieve_job(
                                    $glcRetrieveJobs->{'time'},
                                    $glcRetrieveJobs->{'archive_id'},
                                    $glcRetrieveJobs->{'job_id'}
                                );

        } # for

    } # if

    $self->_index_archives_as_files();

    return;
        
} # end sub read_journal

=item C<open_for_write>

This method overrides parent and does nothing, as we initialize SQLite connection
in constructor

=cut

sub open_for_write {} # end sub open_for_write

=item C<close_for_write>

This method overrides parent and does nothing, as we close SQLite connection
in destructor

=cut

sub close_for_write {} # end sub close_for_write

=item C<add_entry>

Adds entry to the journal

param:

    %params - as superclass App::MtAws::Journal
    
return:

    void
    
=cut

sub add_entry {

    my ($self, $e) = @_;

    confess unless $self->{output_version} eq 'B';

    # TODO: time should be ascending?

    if ($e->{type} eq 'CREATED') {
    
        #" CREATED $archive_id $data->{filesize} $data->{final_hash} $data->{relfilename}"
        for my $field (qw/time archive_id size treehash relfilename/) {
            if( !defined( $e->{$field} ) ) {
                $self->{'infoLogger'}->debug("Bad $field");
                confess "bad $field";
            } # if
        } # for
        
        if(! is_relative_filename($e->{relfilename}) ) {
            $self->{'infoLogger'}->debug("Invalid filename. file: " . $e->{'relfilename'});
            confess "invalid filename";
        } # if
        
        my $mtime = defined($e->{mtime}) ? $e->{mtime} : 'NONE';
    
        my $file = fileparse($e->{'relfilename'}, '.xb.*');
                
        $self->{'debugLogger'}->debug('Selecting history records for uuid: ', $file);
        
        my $query = "SELECT * FROM history WHERE uuid='" . $file . "'";
        
        $self->{'debugLogger'}->debug('Query: ', $query);
        
        my @historyRecs = @{ $self->{'dbh'}->selectall_arrayref($query, { Slice => {} }) };

        $self->{'debugLogger'}->debug('Dumping results: ', sub{ Dumper(\@historyRecs) });
        
        if( scalar(@historyRecs) > 1 ) {
            $self->{'debugLogger'}->debug("There is more than one record for uuid: " . $file);
            croak "There is more than one record for uuid: " . $file;
        } # if
        
        if( scalar(@historyRecs) > 1) {
            $self->{'debugLogger'}->debug("There is no record in history for uuid: " . $file);
            croak "There is no record in history for uuid: " . $file;
        } # if
        
        $self->hashInsert('table' => 'glacier', 'data' => $historyRecs[0]);
        
        my $glacId = $self->{'dbh'}->last_insert_id("", "", "host", "");
        
        $e->{'history_id'} = $historyRecs[0]->{'history_id'};
        $e->{'glacier_id'} = $glacId;
        
        $self->hashInsert('table' => 'journal', 'data' => $e);
        
        #$self->_write_line("B\t$e->{time}\tCREATED\t$e->{archive_id}\t$e->{size}\t$mtime\t$e->{treehash}\t$e->{relfilename}");
    } elsif ($e->{type} eq 'DELETED') {
        #  DELETED $data->{archive_id} $data->{relfilename}     
        for my $field (qw/archive_id relfilename/) {
            if( !defined( $e->{$field} ) ) {
                $self->{'infoLogger'}->debug("Bad $field");
                confess "bad $field";
            } # if
        } # for
        
        if(! is_relative_filename($e->{relfilename}) ) {
            $self->{'infoLogger'}->debug("Invalid filename. file: " . $e->{'relfilename'});
            confess "invalid filename";
        } # if

        my $file = fileparse($e->{'relfilename'}, '.xb.*');
        
        $self->{'debugLogger'}->debug("Selecting all glacier, history id's for archive_id: " . $e->{'archive_id'});
        
        my $query = "SELECT glacier.glacier_id, glacier.history_id FROM glacier JOIN journal ON";
        $query .= " glacier.glacier_id=journal.glacier_id";
        $query .= " WHERE archive_id='" . $e->{'archive_id'} . "'";
        
        $self->{'debugLogger'}->debug('Query: ', $query);
        
        my @historyRecs = @{ $self->{'dbh'}->selectall_arrayref($query, { Slice => {} }) };
        
        $self->{'debugLogger'}->debug("Dumping results: ", sub{ Dumper(\@historyRecs) });
        
        $e->{'history_id'} = $historyRecs[0]->{'history_id'};
        $e->{'glacier_id'} = $historyRecs[0]->{'glacier_id'};
        
        $self->hashInsert('table' => 'journal', 'data' => $e);
        
        #$self->_write_line("B\t$e->{time}\tDELETED\t$e->{archive_id}\t$e->{relfilename}");
    } elsif ($e->{type} eq 'RETRIEVE_JOB') {
        #  RETRIEVE_JOB $data->{archive_id}
        for my $field (qw/archive_id job_id/) {
            if( !defined( $e->{$field} ) ) {
                $self->{'infoLogger'}->debug("Bad $field");
                confess "bad $field";
            } # if
        } # for
        
        my $query = "SELECT glacier.glacier_id, glacier.history_id FROM glacier JOIN journal ON";
        $query .= " glacier.glacier_id=journal.glacier_id";
        $query .= " WHERE archive_id='" . $e->{'archive_id'} . "'";
       
        $self->{'debugLogger'}->debug('Query: ', $query);
        
        my @journalRecs = @{ $self->{'dbh'}->selectall_arrayref($query, { Slice => {} }) };
        
        $self->{'debugLogger'}->debug("Dumping results: ", sub{ Dumper(\@journalRecs) });
        
        $e->{'history_id'} = $journalRecs[0]->{'history_id'};
        $e->{'glacier_id'} = $journalRecs[0]->{'glacier_id'};
        
        $self->hashInsert('table' => 'journal', 'data' => $e);
        
        #$self->_write_line("B\t$e->{time}\tRETRIEVE_JOB\t$e->{archive_id}\t$e->{job_id}");
    } else {
        my $error = @_ || $_;
        $self->{'infoLogger'}->error("Error: ", $error);
        croak "Error: " . $error;
    } # if
        
} # end sub add_entry

=item C<hashInsert>

Simple method for inserting information in hash to SQLite, keys are column names
values are row values

param:

    table string - name of the table to which we want to insert
    
    data hash_ref - data we want to insert
    
return:

    void
    
=cut

sub hashInsert {

    my $self = shift;
    my %params = @_;
    my $table = $params{'table'};
    my $data = $params{'data'};
    
    # inserting info about file uploaded to glacier
    my @values = values(%$data);
    my @escVals = map { my $s = $_; $s = $self->{'dbh'}->quote($s); $s } @values;

    my $query = "INSERT INTO " . $table . " (" . join( ",", keys(%$data) ) . ")";
    $query .= " VALUES(" . join( ",", @escVals ) . ")";

    $self->{'debugLogger'}->debug("Query: ", $query);
    
    try {
        my $sth = $self->{'dbh'}->prepare($query);
        $sth->execute();
    } catch {
        my $error = @_ || $_;
        $self->{'infoLogger'}->error("Error: ", $error);
        croak "Error: " . $error;
    }; # try
    
} # end sub hashInsert

=item C<DESTROY>

deconstructor method, closes SQLite connections

param:

return:

    void
    
=back

=cut

sub DESTROY {
      my $self = shift;
      $self->{'dbh'}->disconnect();
} # end sub DESTROY
  
=head1 AUTHOR

    PAVOL IPOTH <pavol.ipoth@gmail.com>

=head1 COPYRIGHT

    Pavol Ipoth, ALL RIGHTS RESERVED, 2015

=head1 License

    GPLv3

=cut

1;