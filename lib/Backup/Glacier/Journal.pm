package Backup::Glacier::Journal;

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
            
            my $dbh = DBI->connect(
                        "dbi:SQLite:dbname=" . $self->{'journal_file'},
                        "", 
                        "",
                        {'RaiseError' => 1}
                    );

            my $deletedQuery = "SELECT history_id FROM journal";
            $deletedQuery .= " WHERE type='DELETED'";

            my $query = "SELECT * FROM journal WHERE history_id NOT IN";
            $query .= " (" . $deletedQuery . ") AND type='CREATED' ORDER BY mtime ASC";
            print $query;
            try {
                @glcBkpsPresent = @{ $dbh->selectall_arrayref($query, { Slice => {} }) };
            } catch {
                confess @_ || $_;
            };

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
            
            my $retrieveQuery = "SELECT * FROM journal WHERE type='RETRIEVE_JOB'";
            
            try {
                @glcRetrieveJobs = @{ $dbh->selectall_arrayref($retrieveQuery, { Slice => {} }) };
            } catch {
                confess @_ || $_;
            };
            
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

sub open_for_write {

    my $self = shift;
    
    my $dbh = DBI->connect(
                "dbi:SQLite:dbname=" . $self->{'journal_file'},
                "", 
                "",
                {'RaiseError' => 1}
            );
            
    $self->{'dbh'} = $dbh;
    
} # end sub open_for_write

sub close_for_write {
    my $self = shift;  
    $self->{'dbh'}->disconnect();
} # end sub close_for_write

sub add_entry {

    my ($self, $e) = @_;

    confess unless $self->{output_version} eq 'B';

    # TODO: time should be ascending?

    if ($e->{type} eq 'CREATED') {
    
        #" CREATED $archive_id $data->{filesize} $data->{final_hash} $data->{relfilename}"
        defined( $e->{$_} ) || confess "bad $_" for (qw/time archive_id size treehash relfilename/);
        confess "invalid filename" unless is_relative_filename($e->{relfilename});
        
        my $mtime = defined($e->{mtime}) ? $e->{mtime} : 'NONE';
    
        my $file = fileparse($e->{'relfilename'}, '.xb.*');
        
        my $query = "SELECT * FROM history WHERE uuid='" . $file . "'";
        my @historyRecs = @{ $self->{'dbh'}->selectall_arrayref($query, { Slice => {} }) };

        if( scalar(@historyRecs) > 1 ) {
            croak "There is more than one record for uuid: " . $file;
        } # if
        
        if( scalar(@historyRecs) > 1) {
            croak "There is no record in history for uuid: " . $file;
        } # if
        
        $self->hashInsert('table' => 'glacier', 'data' => $historyRecs[0]);
        
        $e->{'history_id'} = $historyRecs[0]->{'history_id'};
        
        $self->hashInsert('table' => 'journal', 'data' => $e);
        
        #$self->_write_line("B\t$e->{time}\tCREATED\t$e->{archive_id}\t$e->{size}\t$mtime\t$e->{treehash}\t$e->{relfilename}");
    } elsif ($e->{type} eq 'DELETED') {
        #  DELETED $data->{archive_id} $data->{relfilename}
        defined( $e->{$_} ) || confess "bad $_" for (qw/archive_id relfilename/);
        confess "invalid filename" unless is_relative_filename($e->{relfilename});

        my $file = fileparse($e->{'relfilename'}, '.xb.*');
        
        my $query = "SELECT glacier.history_id FROM glacier JOIN journal ON";
        $query .= " glacier.history_id=journal.history_id";
        $query .= " WHERE archive_id='" . $e->{'archive_id'} . "'";
        my @historyRecs = @{ $self->{'dbh'}->selectall_arrayref($query, { Slice => {} }) };
        
        $e->{'history_id'} = $historyRecs[0]->{'history_id'};
        
        $self->hashInsert('table' => 'journal', 'data' => $e);
        
        #$self->_write_line("B\t$e->{time}\tDELETED\t$e->{archive_id}\t$e->{relfilename}");
    } elsif ($e->{type} eq 'RETRIEVE_JOB') {
        #  RETRIEVE_JOB $data->{archive_id}
        defined( $e->{$_} ) || confess "bad $_" for (qw/archive_id job_id/);
        
        my $query = "SELECT glacier.history_id FROM glacier JOIN journal ON";
        $query .= " glacier.history_id=journal.history_id";
        $query .= " WHERE archive_id='" . $e->{'archive_id'} . "'";
        my @journalRecs = @{ $self->{'dbh'}->selectall_arrayref($query, { Slice => {} }) };
        
        $e->{'history_id'} = $journalRecs[0]->{'history_id'};
        
        $self->hashInsert('table' => 'journal', 'data' => $e);
        
        #$self->_write_line("B\t$e->{time}\tRETRIEVE_JOB\t$e->{archive_id}\t$e->{job_id}");
    } else {
        confess "Unexpected else";
    } # if
        
} # end sub add_entry

sub hashInsert {

    my $self = shift;
    my %params = @_;
    my $table = $params{'table'};
    my $data = $params{'data'};
    
    # inserting info about file uploaded to glacier
    my @values = values(%$data);
    my @escVals = map { my $s = $_; $s = $self->{'dbh'}->quote($s); $s } @values;

    $query = "INSERT INTO " . $table . " (" . join( ",", keys(%$data) ) . ")";
    $query .= " VALUES(" . join( ",", @escVals ) . ")";

    print $query;
    my $sth = $self->{'dbh'}->prepare($query);
    $sth->execute();
        
} # end sub hashInsert

1;