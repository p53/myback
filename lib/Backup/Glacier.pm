package Backup::Glacier;

use Moose;
use namespace::autoclean;
use Carp;
use Try::Tiny;
use warnings;
use autodie;
use File::Glob;
use File::Copy;
use File::Path;
use File::Basename;
use Data::Dumper;
use DBI;
use YAML::Tiny;
use File::stat;
use Text::SimpleTable;
use App::MtAws::Command::Sync;
use App::MtAws::Command::Retrieve;
use App::MtAws::ForkEngine  qw/with_forks fork_engine/;
use App::MtAws::QueueJob::Iterator;
use App::MtAws::QueueJob::Delete;
use App::MtAws::GlacierRequest;
use App::MtAws;

use Term::Shell;
use Backup::Glacier::Journal;

with 'Backup::BackupInterface', 'MooseX::Log::Log4perl';

sub sync {

    my $self = shift;
    my %params = @_;
    my $bkpDir = $params{'bkpDir'};
    my $config = $params{'config'};
    my $shell = Term::Shell->new();
    my $result = {};
    my @backupsInfo = ();
    
    $config->{'new'} = 1;
    
    my %journal_opts = ( 'journal_encoding' => 'UTF-8' );
    
    my $j = Backup::Glacier::Journal->new(
                                      %journal_opts, 
                                      'journal_file' => $config->{'journal'}, 
                                      'root_dir' => $bkpDir
                                  );

    $self->log('base')->info("Syncing files to glacier");
    
    try {
        $self->log('debug')->debug("Syncing files to glacier with config: ", sub { Dumper($config) });
        App::MtAws::Command::Sync::run($config, $j);
    } catch {
        croak @_ || $_;
        exit(1);
    };
    
} # end sub sync

sub list {

    my $self = shift;
    my %params = @_;
    my $config = $params{'config'};
    my $format = $params{'format'};
    my $bkpDir = $params{'bkpDir'};
    
    my %journal_opts = ( 'journal_encoding' => 'UTF-8' );
    
    my $j = Backup::Glacier::Journal->new(
                                      %journal_opts, 
                                      'journal_file' => $config->{'journal'}, 
                                      'root_dir' => $bkpDir
                                  );
      
    $j->read_journal('should_exist' => 0);
    
    my $data = $j->{'archive_h'};
    my @showData = values($data);
    
    $self->$format('data' => \@showData);

} # end sub list

sub clean {

    my $self = shift;
    my %params = @_;
    my $time = $params{'time'};
    my $config = $params{'config'};
    my $bkpDir = $params{'bkpDir'};
    my @deletedBackups = ();

    my %journal_opts = ( 'journal_encoding' => 'UTF-8' );
    
    my $j = Backup::Glacier::Journal->new(
                                      %journal_opts, 
                                      'journal_file' => $config->{'journal'}, 
                                      'root_dir' => $bkpDir
                                  );
                                  
    my $now = DateTime->now();
    $now->set_time_zone('UTC');
    my $cleanTime = $now->subtract( days => $time );
    
    $self->log('base')->info("Selecting old files from history");
    
    my $query = "SELECT * from history WHERE start_time <";
    $query .= " DATETIME(" . $cleanTime->epoch . ", 'unixepoch')";
    
    $self->log('debug')->debug("Query: ", $query);
        
    my @localBackups = @{ $self->localDbh->selectall_arrayref($query, { Slice => {} }) };
    
    $self->log('debug')->debug("Found backups older than " . $time . " days: ", sub { Dumper(\@localBackups) });
        
    $self->log('base')->info("Starting cleanup");
    
    for my $localBackup(@localBackups) {
        
        $self->log('base')->info("Checking if file with uuid present on filesystem: ", $localBackup->{'uuid'});
        
        my @files = glob($bkpDir . "/*/*/" . $localBackup->{'uuid'} . '*');
        
        $self->log('debug')->debug("Files found: ", sub { Dumper(\@files) });
        
        my $deletedQuery = "SELECT history_id FROM journal";
        $deletedQuery .= " WHERE type='DELETED'";

        my $glcQuery = "SELECT COUNT(journal_id) AS rec_count FROM journal WHERE";
        $glcQuery .= " history_id NOT IN (" . $deletedQuery . ") AND type='CREATED'";
        $glcQuery .= " AND relfilename LIKE '%" . $localBackup->{'uuid'}. "%'";
        
        my $delQuery = "DELETE FROM history WHERE uuid='" . $localBackup->{'uuid'}. "'";
                    
        $self->log('base')->info("Checking if uuid " . $localBackup->{'uuid'} . " is already in glacier");
        $self->log('debug')->debug("Query: ", $glcQuery);
        
        my @countRecs = @{ $self->localDbh->selectall_arrayref($glcQuery, { Slice => {} }) };
        
        $self->log('debug')->debug("Found backups in glacier: ", $countRecs[0]->{'rec_count'});
        
        if( scalar(@files) > 1 ) {
            $self->log('base')->info("Found several files with same uuid: ", $localBackup->{'uuid'});
            croak "Found several files with same uuid: " . $localBackup->{'uuid'};
        } # if
       
        if( scalar(@files) == 1 && $countRecs[0]->{'rec_count'} > 0) {
        
            $self->log('base')->info("File found on filesystem and also in glacier, removing local file: ", $files[0]);
            
            unlink $files[0];
            
            $self->log('base')->info("Removing from history: ", $localBackup->{'uuid'});
            
            my $sth = $self->localDbh->prepare($delQuery);
            $sth->execute();
            
            push(@deletedBackups, $localBackup);
            
        } elsif( scalar(@files) == 0 && $countRecs[0]->{'rec_count'} == 0) {
            $self->log('base')->info("File not on filesytem nor in glacier, removing from history: ", $localBackup->{'uuid'});
            my $sth = $self->localDbh->prepare($delQuery);
            $sth->execute();
            push(@deletedBackups, $localBackup);
        } # if
        
    } # for

    $self->log('debug')->debug("Cleaned up entries: ", sub { Dumper(\@deletedBackups) });
    
    return \@deletedBackups;
    
} # end sub clean

sub clean_rmt {

    my $self = shift;
    my %params = @_;
    my $time = $params{'time'};
    my $config = $params{'config'};
    my $bkpDir = $params{'bkpDir'};
    
    my %journal_opts = ( 'journal_encoding' => 'UTF-8' );
    
    my $j = Backup::Glacier::Journal->new(
                                      %journal_opts, 
                                      'journal_file' => $config->{'journal'}, 
                                      'root_dir' => $bkpDir
                                  );
                                  
    my $now = DateTime->now();
    $now->set_time_zone('UTC');
    my $cleanTime = $now->subtract( days => $time );
    
    $self->log('base')->info("Selecting old files from history");
    
    my $deletedQuery = "SELECT history_id FROM journal";
    $deletedQuery .= " WHERE type='DELETED'";

    my $glcQuery = "SELECT * FROM journal JOIN glacier ON";
    $glcQuery .= " journal.history_id = glacier.history_id WHERE";
    $glcQuery .= " history_id NOT IN (" . $deletedQuery . ") AND type='CREATED'";
    $glcQuery .= " AND glacier.start_time < DATETIME(" . $cleanTime->epoch . ", 'unixepoch')";
    
    $self->log('debug')->debug("Query: ", $glcQuery);
        
    my @glcBackups = @{ $self->localDbh->selectall_arrayref($glcQuery, { Slice => {} }) };
    
    $self->log('debug')->debug("Found backups older than " . $time . " days: ", sub { Dumper(\@glcBackups) });
        
    $self->log('base')->info("Starting cleanup");
       
    if( scalar(@glcBackups) > 0 ) {
    
        $j->open_for_write();

        my @deleteFiles = map { $_->{'history_id'} } @glcBackups;

        my @filelist = map { 
                                {
                                   'archive_id' => $_->{'archive_id'}, 
                                   'relfilename' => $_->{'relfilename'}
                                } 
                            } @glcBackups;
                            
        with_forks(1, $config, sub {
        
            my $ft = App::MtAws::QueueJob::Iterator->new(iterator => sub {
                    if (my $rec = shift @filelist) {
                            return App::MtAws::QueueJob::Delete->new(
                                    'relfilename' => $rec->{'relfilename'}, 
                                    'archive_id' => $rec->{'archive_id'}
                            );
                    } else {
                            return;
                    }
            });

            my ($R) = fork_engine->{'parent_worker'}->process_task($ft, $j);

            die unless $R;
            
        });
        
        $self->log('base')->info("Deleting entries from glacier history");

        my $deleteQuery = "DELETE FROM glacier WHERE history_id IN";
        $deleteQuery .= " (" . join(',', @deleteFiles) . ")";
        my $sth = $j->{'dbh'}->prepare($deleteQuery);
        $sth->execute();

        $j->close_for_write();
    
    } # if
    
    $self->log('debug')->debug("Cleaned up entries: ", sub { Dumper(\@glcBackups) });
    
    return \@glcBackups;
    
} # end sub clean_rmt

sub get {
    
    my $self = shift;
    my %params = @_;
    my $time = $params{'time'};
    my $uuid = $params{'uuid'};
    my $config = $params{'config'};
    my $bkpDir = $params{'bkpDir'};
    my $chain = [];        
    my %journal_opts = ( 'journal_encoding' => 'UTF-8' );
    
    my $j = Backup::Glacier::Journal->new(
                                      %journal_opts, 
                                      'journal_file' => $config->{'journal'}, 
                                      'root_dir' => $bkpDir
                                  );

    $self->log('base')->info("Getting info about restored backup with uuid: " . $uuid);
    
    my $backups = $self->getGlacierBackupsInfo('uuid' => $uuid);
    
    $self->log('debug')->debug("Dumping backup info: ", sub { Dumper($backups) });
    
    if( scalar(@$backups) > 1 ) {
        croak "Found more than one entry for uuid in glacier history: " . $uuid;
    } # if
    
    if( $backups->[0]->{'incremental'} eq 'Y' ) {
    
        my $backupsInfo = {};
        my $allBackups = $self->getGlacierBackupsInfo();

        for my $bkp(@$allBackups) {
            $backupsInfo->{$bkp->{'uuid'}} = $bkp;
        } # for

        if( !defined( $backupsInfo->{$uuid} ) ) {
            $self->log->error("No backups with uuid $uuid!");
            croak "No backups with uuid $uuid!";
        } # if
        
        $self->log('base')->info("Getting backups info till nearest previous full backup");

        # to be able to restore incremental backup, we need previous incremental
        # backups plus full backup and we are getting this info here, backups are
        # returned from newest to oldest
        $chain = $self->getGlacierBackupChain(
                                        'backupsInfo' => $backupsInfo, 
                                        'uuid' => $uuid, 
                                        'chain' => $chain
                                    );
        
    } # if
    
    push(@$chain, $backups->[0]);

    $self->log('debug')->debug("Dumping backup chain info: ", sub { Dumper($chain) });
    
    for my $chainBackup(@$chain) {

        my $restoredFile = $j->absfilename($chainBackup->{'relfilename'});
        my ($file, $directory) = fileparse($restoredFile, '.xb.*');

        if( ! -d $directory ) {
            $self->log('base')->info("Creating restore directory: " . $directory);
            mkpath($directory);
        } # if

    } # for
        
    $self->log('base')->info("Retrieving archives");
    
    my @backupChain = @$chain;
    
    with_forks( !$config->{'dry-run'}, $config, sub {
        my $ft = App::MtAws::QueueJob::Iterator->new(iterator => sub {
            if (my $rec = shift @$chain) {
                return App::MtAws::QueueJob::Retrieve->new(
                        'relfilename' => $rec->{'relfilename'}, 
                        'archive_id' => $rec->{'archive_id'},
                        'filename' => $j->absfilename($rec->{'relfilename'})
                );
            } else {
                return;
            } # if
        });

        $j->open_for_write();
        my ($R) = fork_engine->{parent_worker}->process_task($ft, $j);
        die unless $R;
        $j->close_for_write();
    });
    
    my %filelist = map { 
                            $_->{'filename'} = $j->absfilename($_->{'relfilename'});
                            $_->{'archive_id'} => $_ 
                        } @backupChain;
    
    my $downloadTimeout = 2 * 3600;
    my $iteration = 0;
    my $interval = 5 * 60;
    
    while( $iteration <= $downloadTimeout ) {
    
        $self->log('base')->info("Downloading archives");
            
        with_forks( !$config->{'dry-run'}, $config, sub {
            my $fad = App::MtAws::QueueJob::FetchAndDownload->new(
                                                                    'file_downloads' => {}, 
                                                                    'archives' => \%filelist
                                                                );
            my ($H) = fork_engine->{'parent_worker'}->process_task($fad, $j);
            die unless $H;
        });
        
        $self->log('base')->info("Checking if archives were downloaded, iteration is: ", $iteration, " seconds");
            
        my $presence = {};
        my $unpresent = {};
        
        for my $downFile( values(%filelist) ) {
            if( -f $downFile->{'filename'} ) {
                $presence->{$downFile->{'archive_id'}} = $downFile;
            } else {
                $unpresent->{$downFile->{'archive_id'}} = $downFile;
            }# if
        } # for

        $self->log('debug')->debug("Not yet downloaded archives: ", sub { Dumper($unpresent) });
        
        if( scalar( values(%filelist) ) == scalar( values(%$presence) )) {
            $self->log('base')->info("All archives downloaded");
            last;
        } else {
            if( $iteration == 0 ) {
                $self->log('base')->info("Waiting 4 hours to retrieve");
                sleep 4 * 3600;
            } else {
                sleep $interval;
            }
            $iteration += $interval;
            next;
        } # if
        
        $self->log->error("Timeout for download " . $downloadTimeout . " expired!");
        $self->log->error("Not able to download these: ", sub{ Dumper($unpresent)});
        croak "Timeout for download " . $downloadTimeout . " expired!";
        
    } # while
    
} # end sub get

sub tbl {

    my $self = shift;
    my %params = @_;
    my $data = $params{'data'};

    my @units = ('b','Kb','Mb','Gb','Tb','Pb','Eb');
    
    my $bkpTbl = Text::SimpleTable->new(
                                        [19, 'time'],
                                        [19, 'mtime'],
                                        [6, 'size'],
                                        [12, 'treehash'],
                                        [19, 'relfilename'],
                                        [12, 'archive_id']
                                    );
    
    for my $info(@$data) {
    
        my $sizeLength = length($info->{'size'});
        my $order = int($sizeLength / 3);
        $order = ($sizeLength % 3) > 0 ? $order : ($order -1);
        my $convUnit = ($order < 0) ? '' : $units[$order];
        
        my $converted = $info->{'size'} >> ( $order * 10 );
        
        my $time = DateTime->from_epoch( epoch => $info->{'time'} );
        my $mtime = DateTime->from_epoch( epoch => $info->{'mtime'} );
        
        $bkpTbl->row(
                        $time->ymd('-') . ' ' . $time->hms(':'),
                        $mtime->ymd('-') . ' ' . $mtime->hms(':'),
                        $converted . $convUnit,
                        $info->{'treehash'},
                        $info->{'relfilename'},
                        $info->{'archive_id'}
                    );
        $bkpTbl->hr;
        
    } # for

    print $bkpTbl->draw;

} # end sub tbl

sub lst {

    my $self = shift;
    my %params = @_;
    my $data = $params{'data'};

    my $bkpTbl = Text::SimpleTable->new(
                                        [70, 'Backup Info'],
                                    );

    for my $info(@$data) {

        my $row = '';

        while ( my ($key,$value) = each %$info) {
            $row .= $key . ': ' . $value . "\n";
        } # while

        $bkpTbl->row(
                        $row
                    );
        $bkpTbl->hr;

    } # for

    print $bkpTbl->draw;

} # end sub lst

sub getGlacierBackupsInfo {

    my $self = shift;
    my %params = @_;
    my $uuid = $params{'uuid'};
    my @backupsInfo = ();

    $self->log('debug')->debug("Getting remote backups info with params: ", , sub { Dumper(\%params) });
   
    my $deletedQuery = "SELECT journal.history_id FROM journal";
    $deletedQuery .= " WHERE type='DELETED'";
    
    my $getQuery = "SELECT * FROM journal JOIN glacier ON";
    $getQuery .= " journal.history_id = glacier.history_id WHERE";
    $getQuery .= " journal.history_id NOT IN (" . $deletedQuery . ") AND type='CREATED'";
    
    if( $uuid ) {
        $getQuery .= " AND glacier.uuid='" . $uuid . "'";
    } # if
    
    $self->log('debug')->debug("Query: ", $getQuery);
    
    @backupsInfo = @{ $self->localDbh->selectall_arrayref($getQuery, { Slice => {} }) };

    return \@backupsInfo;

} # end sub getRmtBackupsInfo

sub getGlacierBackupChain {

    my $self            = shift;
    my %params          = @_;
    my $backupsInfo     = $params{'backupsInfo'};
    my $uuid            = $params{'uuid'};
    my $chain           = $params{'chain'};
    my $currentBackup   = $backupsInfo->{$uuid};

    # we reduce array of backups by previous uuid
    delete $backupsInfo->{$uuid};

    my @candidates = ();
    my $closestCandidate = {};

    # we want to find backup which has start lsn same as end_lsn of previous
    # backup
    for my $backup(values %$backupsInfo) {    

        if( $backup->{'innodb_to_lsn'} == $currentBackup->{'innodb_from_lsn'} ) {
            push(@candidates, $backup);
        } # if

    } # for

    # if there are more than one such backups, we sort them and choose the
    # one which is in time closest to our previous backup
    if( scalar(@candidates) > 1 ) {
        
        my %timeDiffs = ();
        
        for my $candidate(@candidates) {
            my $diff = $currentBackup->{'start_unix_time'} - $candidate->{'start_unix_time'};
            $timeDiffs{$diff} = $candidate;
        } # for

        my @sortedDiffs = sort{ $a <=> $b } keys %timeDiffs;
        my $minDiff = $sortedDiffs[scalar(@sortedDiffs) - 1];

        $closestCandidate = $timeDiffs{$minDiff};

    } else {
        $closestCandidate = $candidates[0];
    } # if

    push(@$chain, $closestCandidate);
    
    if( ! defined($closestCandidate->{'incremental'}) ) {
        croak "Invalid data, missing incremental field, hint: missing backup in backup chain?";
    } # if

    # if we didn't reach full backup we need to find it's parent
    if( $closestCandidate->{'incremental'} eq 'Y' ) {
        $self->getGlacierBackupChain(
                                'backupsInfo' => $backupsInfo, 
                                'uuid' => $closestCandidate->{'uuid'}, 
                                'chain' => $chain
                            );
    } # if

    return $chain;

} # end sub getGlacierBackupChain

no Moose::Role;

=head1 AUTHOR

        PAVOL IPOTH <pavol.ipoth@gmail.com>

=head1 COPYRIGHT

        Pavol Ipoth, ALL RIGHTS RESERVED, 2015

=head1 License

        GPLv3

=cut

1;
