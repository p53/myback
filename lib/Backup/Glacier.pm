package Backup::Glacier;

=head1 NAME

    Backup::Glacier - module for managing/uploading files to glacier AWS service

=head1 SYNOPSIS

    my $backupObj = Backup::Glacier->new();
    my $configEngine = App::MtAws::ConfigEngine->new();
    my $config = $configEngine->read_config($glcConfigPath);
    
    $backupObj->sync(     
                            'config' => $config,
                            'bkpDir' => '/backups',
                        );

=cut

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
use POSIX;
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
use Backup::Type::Full;
use Backup::Type::Incremental;

with 'Backup::BackupInterface',
     'MooseX::Log::Log4perl';

=head1 METHODS

=over 12

=item C<sync>

Syncs files in backup directory with amazon glacier, uploads only files which arent
already in glacier and if they have different mtime

param:

    bkpDir string - requried parameter, location of backups
    
    config hash_ref - config options

return:

    void

=cut

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
    
    # journal is used by App::MtAws library to track files, which are already
    # in glacier and to track jobs
    my $j = Backup::Glacier::Journal->new(
                                      %journal_opts, 
                                      'journal_file' => $config->{'journal'}, 
                                      'root_dir' => $bkpDir,
                                      'debugLogger' => $self->log('debug'),
                                      'infoLogger' => $self->log('base')
                                  );

    $self->log('base')->info("Syncing files to glacier");
    
    try {
    
        $self->log('debug')->debug("Syncing files to glacier with config: ", sub { Dumper($config) });
        
        # use from App::MtAws library, creates workers to upload files
        with_forks(!$config->{'dry-run'}, $config, sub {
        
            my $read_journal_opts = {'new' => 1};
            my @joblist;
            
            $j->read_journal(should_exist => 0);
            $j->read_files($read_journal_opts, $config->{'max-number-of-files'});
            $j->open_for_write();
            
            if ($config->{new}) {
                my $itt = sub { 
                    if (my $rec = shift @{ $j->{'listing'}{'new'} }) {
                        my $absfilename = $j->absfilename($rec->{'relfilename'});
                        my $relfilename = $rec->{'relfilename'};
                        my $size = stat($absfilename)->size;
                        my $partSize = $self->calcPartSize('size' => $size, 'config' => $config);
                        App::MtAws::QueueJob::Upload->new(
                            'filename' => $absfilename, 
                            'relfilename' => $relfilename, 
                            'partsize' => $partSize, 
                            'delete_after_upload' => 0
                        );
                    } else {
                            return;
                    } # if
                };
                push @joblist, App::MtAws::QueueJob::Iterator->new(iterator => $itt);
            } # if

            if (scalar @joblist) {
                    my $lt = do {
                        confess unless @joblist >= 1;
                        App::MtAws::QueueJob::Iterator->new(iterator => sub { shift @joblist });
                    };
                    my ($R) = fork_engine->{parent_worker}->process_task($lt, $j);
                    confess unless $R;
            } # if
            
            $j->close_for_write();
            
	});
        
    } catch {
        my $error = @_ || $_;
        $self->log('base')->error("There was some problem: ", $error);
        croak $error;
        exit(1);
    };
    
} # end sub sync

=item C<list>

method lists all backups which are in glacier

param:

    bkpDir string - requried parameter, location of backups
    
    config hash_ref - config options
    
    format - output format
    
return:

    void
    
=cut

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
                                      'root_dir' => $bkpDir,
                                      'debugLogger' => $self->log('debug'),
                                      'infoLogger' => $self->log('base')
                                  );
      
    $j->read_journal('should_exist' => 0);
    my $data = $j->{'archive_sorted'};
    
    $self->$format('data' => $data);

} # end sub list

=item C<clean>

Method removes files older than time (according history table) on local filesystem, 
if file is on fs and present in glacier, if it is not in glacier nor fs, just history entry
will be removed. If file is just on fs, but not in glacier, it wont be removed
           
param:

    bkpDir string - requried parameter, location of backups
    
    config hash_ref - config options
    
    time - number of days, hours, minutes - determines how old entries/files
           should be selected

return:

    $deletedBackups array_ref - list of deleted backups hash_refs
    
=cut

sub clean {

    my $self = shift;
    my %params = @_;
    my $time = $params{'time'};
    my $config = $params{'config'};
    my $bkpDir = $params{'bkpDir'};
    my @deletedBackups = ();
    my $units = {
                    'h' => 'hours',
                    'd' => 'days',
                    'm' => 'minutes'
                };
    
    $time =~ /(\d+)([hdm])/;
    my $unit = $2;
    my $timeNum = $1;
    
    if( ! exists($units->{$unit}) ) {
        $self->log('base')->error("Bad time unit: ", $unit);
        croak "Bad time unit!";
    } # if
    
    my %journal_opts = ( 'journal_encoding' => 'UTF-8' );
    
    my $j = Backup::Glacier::Journal->new(
                                      %journal_opts, 
                                      'journal_file' => $config->{'journal'}, 
                                      'root_dir' => $bkpDir,
                                      'debugLogger' => $self->log('debug'),
                                      'infoLogger' => $self->log('base')
                                  );
                                  
    my $now = DateTime->now();
    $now->set_time_zone('UTC');
    
    # we get time now minus number of days/hours/minutes
    my $cleanTime = $now->subtract( $units->{$unit} => $timeNum );
    
    $self->log('base')->info("Selecting old files from history");
    
    # selecting all entries in history which are older than computed time
    my $query = "SELECT * from history WHERE start_time <";
    $query .= " DATETIME(" . $cleanTime->epoch . ", 'unixepoch')";
    
    $self->log('debug')->debug("Query: ", $query);
    
    my @localBackups = ();
    
    try {
        @localBackups = @{ $self->localDbh->selectall_arrayref($query, { Slice => {} }) };
    } catch {
        my $error = @_ || $_;
        $self->log('base')->error("Error: ", $error, " Query: " . $query);
        croak "Error: " . $error;
    };
    
    $self->log('debug')->debug("Found backups older than " . $timeNum . " $units->{$unit}: ", sub { Dumper(\@localBackups) });
        
    $self->log('base')->info("Starting cleanup");
    
    # for each entry/backup which is older than computed time, we check if in journal
    # are entries with type created but not deleted for that backup, that would
    # mean it is present in glacier, we also check if file is present on fs
    # then if file is present on fs and in glacier we remove file on filesystem
    # if it is not present on fs nor glacier, we just remove entry from history
    # of backups, otherwise we do nothing
    for my $localBackup(@localBackups) {
        
        $self->log('base')->info("Checking if file with uuid present on filesystem: ", $localBackup->{'uuid'});
        
        my @files = glob($bkpDir . "/*/*/" . $localBackup->{'uuid'} . '*');
        
        $self->log('debug')->debug("Files found: ", sub { Dumper(\@files) });
        
        my $deletedQuery = "SELECT glacier_id FROM journal";
        $deletedQuery .= " WHERE type='DELETED'";

        my $glcQuery = "SELECT COUNT(journal_id) AS rec_count FROM journal WHERE";
        $glcQuery .= " glacier_id NOT IN (" . $deletedQuery . ") AND type='CREATED'";
        $glcQuery .= " AND relfilename LIKE '%" . $localBackup->{'uuid'}. "%'";
        
        my $delQuery = "DELETE FROM history WHERE uuid='" . $localBackup->{'uuid'}. "'";
                    
        $self->log('base')->info("Checking if uuid " . $localBackup->{'uuid'} . " is already in glacier");
        $self->log('debug')->debug("Query: ", $glcQuery);
        
        my @countRecs = 0;
        
        try {
            @countRecs = @{ $self->localDbh->selectall_arrayref($glcQuery, { Slice => {} }) };
        } catch {
            my $error = @_ || $_;
            $self->log('base')->error("Error: ", $error, " Query: " . $glcQuery);
            croak "Error: " . $error;
        };
        
        $self->log('debug')->debug("Found backups in glacier: ", $countRecs[0]->{'rec_count'});
        
        if( scalar(@files) > 1 ) {
            $self->log('base')->info("Found several files with same uuid: ", $localBackup->{'uuid'});
            croak "Found several files with same uuid: " . $localBackup->{'uuid'};
        } # if
       
        if( scalar(@files) == 1 && $countRecs[0]->{'rec_count'} > 0) {
        
            $self->log('base')->info("File found on filesystem and also in glacier, removing local file: ", $files[0]);
            
            unlink $files[0];
            
            $self->log('base')->info("Removing from history: ", $localBackup->{'uuid'});
            
            try {
                my $sth = $self->localDbh->prepare($delQuery);
                $sth->execute();
            } catch {
                my $error = @_ || $_;
                $self->log('base')->error("Error: ", $error, " Query: " . $delQuery);
                croak "Error: " . $error;
            }; # try
            
            push(@deletedBackups, $localBackup);
            
        } elsif( scalar(@files) == 0 && $countRecs[0]->{'rec_count'} == 0) {
            
            $self->log('base')->info("File not on filesytem nor in glacier, removing from history: ", $localBackup->{'uuid'});
            
            try {
                my $sth = $self->localDbh->prepare($delQuery);
                $sth->execute();
            } catch {
                my $error = @_ || $_;
                $self->log('base')->error("Error: ", $error, " Query: " . $delQuery);
                croak "Error: " . $error;
            }; # try
            
            push(@deletedBackups, $localBackup);
            
        } # if
        
    } # for

    $self->log('debug')->debug("Cleaned up entries: ", sub { Dumper(\@deletedBackups) });
    
    return \@deletedBackups;
    
} # end sub clean

=item C<clean_rmt>

Method removes files older than supplied time (according glacier history table)
from glacier

param:

    bkpDir string - requried parameter, location of backups
    
    config hash_ref - config options
    
    time - number of days, hours, minutes - determines how old entries/files
           should be selected

return:

    $glcBackups array_ref - list of deleted backups hash_refs

=cut

sub clean_rmt {

    my $self = shift;
    my %params = @_;
    my $time = $params{'time'};
    my $config = $params{'config'};
    my $bkpDir = $params{'bkpDir'};
    my $units = {
                    'h' => 'hours',
                    'd' => 'days',
                    'm' => 'minutes'
                };
    
    $time =~ /(\d+)([hdm])/;
    my $unit = $2;
    my $timeNum = $1;
    
    if( ! exists($units->{$unit}) ) {
        $self->log('base')->error("Bad time unit: ", $unit);
        croak "Bad time unit!";
    } # if
    
    my %journal_opts = ( 'journal_encoding' => 'UTF-8' );
    
    my $j = Backup::Glacier::Journal->new(
                                      %journal_opts, 
                                      'journal_file' => $config->{'journal'}, 
                                      'root_dir' => $bkpDir,
                                      'debugLogger' => $self->log('debug'),
                                      'infoLogger' => $self->log('base')
                                  );
                                  
    my $now = DateTime->now();
    $now->set_time_zone('UTC');
    my $cleanTime = $now->subtract( $units->{$unit} => $timeNum );
    
    $self->log('base')->info("Selecting old files from history");
    
    my $deletedQuery = "SELECT glacier_id FROM journal";
    $deletedQuery .= " WHERE type='DELETED'";

    my $glcQuery = "SELECT glacier.glacier_id AS glc_id, * FROM journal JOIN glacier ON";
    $glcQuery .= " journal.glacier_id = glacier.glacier_id WHERE";
    $glcQuery .= " journal.glacier_id NOT IN (" . $deletedQuery . ") AND type='CREATED'";
    $glcQuery .= " AND glacier.start_time < DATETIME(" . $cleanTime->epoch . ", 'unixepoch')";
    
    $self->log('debug')->debug("Query: ", $glcQuery);
        
    my @glcBackups = ();
    
    try {
        @glcBackups = @{ $self->localDbh->selectall_arrayref($glcQuery, { Slice => {} }) };
    } catch {
        my $error = @_ || $_;
        $self->log('base')->error("Error: ", $error, " Query: " . $glcQuery);
        croak "Error: " . $error;
    };
    
    $self->log('debug')->debug("Found backups older than " . $timeNum . " $units->{$unit}: ", sub { Dumper(\@glcBackups) });
        
    $self->log('base')->info("Starting cleanup");
       
    if( scalar(@glcBackups) > 0 ) {
    
        $j->open_for_write();

        my @deleteFiles = map { $_->{'glc_id'} } @glcBackups;

        my @filelist = map { 
                                {
                                   'archive_id' => $_->{'archive_id'}, 
                                   'relfilename' => $_->{'relfilename'}
                                } 
                            } @glcBackups;
        
        try {
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
        } catch {
            my $error = @_ || $_;
            $self->log('base')->error("Error: ", $error);
            croak "Error: " . $error;
        };
        
        $self->log('base')->info("Deleting entries from glacier history");

        my $deleteQuery = "DELETE FROM glacier WHERE glacier_id IN";
        $deleteQuery .= " (" . join(',', @deleteFiles) . ")";
        
        try {
            my $sth = $j->{'dbh'}->prepare($deleteQuery);
            $sth->execute();
        } catch {
            my $error = @_ || $_;
            $self->log('base')->error("Error: ", $error, " Query: " . $deleteQuery);
            croak "Error: " . $error;
        }; # try
        
        $j->close_for_write();
    
    } # if
    
    $self->log('debug')->debug("Cleaned up entries: ", sub { Dumper(\@glcBackups) });
    
    return \@glcBackups;
    
} # end sub clean_rmt

=item C<get>

Method get retrievs and downloads file identified by uuid from glacier to
supplied location

param:

    bkpDir string - requried parameter, location of backups
    
    config hash_ref - config options
    
    uuid string - uuid which identifies file
    
    location string - path where we should download file from glacier
    
return:

=cut

sub get {
    
    my $self = shift;
    my %params = @_;
    my $uuid = $params{'uuid'};
    my $config = $params{'config'};
    my $bkpDir = $params{'bkpDir'};
    my $location = $params{'location'} . '/get';
    my $origLoc = $params{'location'};
    my $chain = [];        
    my %journal_opts = ( 'journal_encoding' => 'UTF-8' );
    
    my $j = Backup::Glacier::Journal->new(
                                      %journal_opts, 
                                      'journal_file' => $config->{'journal'}, 
                                      'root_dir' => $bkpDir,
                                      'debugLogger' => $self->log('debug'),
                                      'infoLogger' => $self->log('base')
                                  );

    $self->log('base')->info("Getting info about restored backup with uuid: " . $uuid);
    
    my $backups = $self->getGlacierBackupsInfo('uuid' => $uuid);
    
    $self->log('debug')->debug("Dumping backup info: ", sub { Dumper($backups) });
    
    if( scalar(@$backups) > 1 ) {
        $self->log('base')->error("Found more than one entry for uuid in glacier history: ", $uuid);
        croak "Found more than one entry for uuid in glacier history: " . $uuid;
    } # if
    
    if( $backups->[0]->{'incremental'} eq 'Y' ) {
        
        $self->log('base')->info("Getting backups info till nearest previous full backup");

        # to be able to restore incremental backup, we need previous incremental
        # backups plus full backup and we are getting this info here, backups are
        # returned from newest to oldest
        $chain = $self->getGlacierBackupChain(
                                        'uuid' => $uuid
                                    );
        
    } else {
        push(@$chain, $backups->[0]);
    } # if
    
    $self->log('debug')->debug("Dumping backup chain info: ", sub { Dumper($chain) });
    
    # for each backup we need to create folder
    for my $chainBackup(@$chain) {

        my $restoredFile = $location . '/' . $chainBackup->{'relfilename'};
        my ($file, $directory) = fileparse($restoredFile, '.xb.*');

        if( ! -d $directory ) {
            $self->log('base')->info("Creating restore directory: " . $directory);
            mkpath($directory);
        } # if

    } # for
        
    $self->log('base')->info("Retrieving archives");
    
    my @backupChain = @$chain;
    
    # first we retrieve files
    try {
        with_forks( !$config->{'dry-run'}, $config, sub {
            my $ft = App::MtAws::QueueJob::Iterator->new(iterator => sub {
                if (my $rec = shift @$chain) {
                    return App::MtAws::QueueJob::Retrieve->new(
                            'relfilename' => $rec->{'relfilename'}, 
                            'archive_id' => $rec->{'archive_id'},
                            'filename' => $location . '/' . $rec->{'relfilename'}
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
    } catch {
        my $error = @_ || $_;
        $self->log('base')->error("Error: ", $error);
        croak "Error: " . $error;
    };
    
    my %filelist = map { 
                            $_->{'filename'} = $location . '/' . $_->{'relfilename'};
                            $_->{'archive_id'} => $_ 
                        } @backupChain;
    
    # we wait 4 hours to retrieve and then try downloadTimeout to download
    # files with interval $interval
    my $downloadTimeout = 2 * 3600;
    my $iteration = 0;
    my $interval = 5 * 60;
    
    while( $iteration <= $downloadTimeout ) {
    
        $self->log('base')->info("Downloading archives");
            
        try {
            with_forks( !$config->{'dry-run'}, $config, sub {
                my $fad = App::MtAws::QueueJob::FetchAndDownload->new(
                                                                        'file_downloads' => {}, 
                                                                        'archives' => \%filelist
                                                                    );
                my ($H) = fork_engine->{'parent_worker'}->process_task($fad, $j);
                die unless $H;
            });
        } catch {
            my $error = @_ || $_;
            $self->log('base')->error("Error: ", $error);
            croak "Error: " . $error;
        };
        
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
            } # if
            $iteration += $interval;
            next;
        } # if
        
        $self->log('base')->error("Timeout for download " . $downloadTimeout . " expired!");
        $self->log('base')->error("Not able to download these: ", sub{ Dumper($unpresent)});
        croak "Timeout for download " . $downloadTimeout . " expired!";
        
    } # while
    
    $self->log('base')->info("Starting restore from downloaded archives");
    
    if( $backups->[0]->{'incremental'} eq 'Y' ) {
        my %restParams = %params;
        $restParams{'hostBkpDir'} = $location . '/*';
        $restParams{'chain'} = \@backupChain;
        $restParams{'location'} = $origLoc . '/restore';
        my $incrBkpObj = Backup::Type::Incremental->new();
        $incrBkpObj->restore_common(%restParams);
    } else {
        my %restParams = %params;
        $restParams{'hostBkpDir'} = $location . '/*';
        $restParams{'location'} = $origLoc . '/restore';
        my $fullBkpObj = Backup::Type::Full->new();
        $fullBkpObj->restore(%restParams);
    } # if
    
    $self->log('base')->info("Restore completed!");
    
} # end sub get

=item C<clean_journal>

Removes entries for archives, which already have delete statement in log older
than specified time

param:

    bkpDir string - requried parameter, location of backups
    
    config hash_ref - config options
    
    time - number of days, hours, minutes - determines how old entries/files
           should be selected
           
return:

=cut

sub clean_journal {

    my $self = shift;
    my %params = @_;
    my $time = $params{'time'};
    my $config = $params{'config'};
    my $bkpDir = $params{'bkpDir'};
    my $units = {
                    'h' => 'hours',
                    'd' => 'days',
                    'm' => 'minutes'
                };
    
    $time =~ /(\d+)([hdm])/;
    my $unit = $2;
    my $timeNum = $1;
    
    if( ! exists($units->{$unit}) ) {
        $self->log('base')->error("Bad time unit: ", $unit);
        croak "Bad time unit!";
    } # if
    
    my %journal_opts = ( 'journal_encoding' => 'UTF-8' );
    
    my $j = Backup::Glacier::Journal->new(
                                      %journal_opts, 
                                      'journal_file' => $config->{'journal'}, 
                                      'root_dir' => $bkpDir,
                                      'debugLogger' => $self->log('debug'),
                                      'infoLogger' => $self->log('base')
                                  );
                                  
    my $now = DateTime->now();
    $now->set_time_zone('UTC');
    my $cleanTime = $now->subtract( $units->{$unit} => $timeNum );
    
    $self->log('base')->info("Selecting journal entries older than ", $timeNum, $units->{$unit});

    # we select achive entries which have delete entry older than specified time
    # and they are not present in glacier history table
    my $subQuery = "SELECT archive_id FROM journal WHERE";
    $subQuery .= " journal.time < " . $cleanTime->epoch . " AND type='DELETED'";
    $subQuery .= " AND glacier_id NOT IN (SELECT glacier_id FROM glacier)";
    $subQuery .= " GROUP BY archive_id";
    
    my $query = "SELECT * FROM journal WHERE archive_id IN (" . $subQuery . ")";
    
    $self->log('debug')->debug("Query: ", $query);
    
    my @deletedEntries = ();
    
    try {
        @deletedEntries = @{ $self->localDbh->selectall_arrayref($query, { Slice => {} }) };
    } catch {
        my $error = @_ || $_;
        $self->log('base')->error("Error: ", $error, " Query: " . $query);
        croak "Error: " . $error;
    };
    
    $self->log('debug')->debug("Dumping old entries from journal: ", sub { Dumper(\@deletedEntries)});
    
    $self->log('base')->info("Deleting old entries from journal");
    
    $query = "DELETE FROM journal WHERE archive_id IN (" . $subQuery . ")";
    
    $self->log('debug')->debug("Query: ", $query);

    try {
        my $sth = $self->localDbh->prepare($query);
        $sth->execute();
    } catch {
        my $error = @_ || $_;
        $self->log('base')->error("Error: ", $error, " Query: " . $query);
        croak "Error: " . $error;
    }; # try
    
} # end sub clean_journal

=item C<tbl>

Method outputs data passed in table format

param:

    data array_ref - list of hashes with information
    
return:

    void
    
=cut

sub tbl {

    my $self = shift;
    my %params = @_;
    my $data = $params{'data'};
    
    my $bkpTbl = Text::SimpleTable->new(
                                        [19, 'relfilename'],
                                        [19, 'start_time'],
                                        [36, 'uuid'],
                                        [10, 'bkp_size'],
                                        [1, 'p'],
                                        [1, 'i'],
                                        [1, 't' ],
                                        [1, 'c' ]
                                    );
    
    my $sum = 0;
    
    for my $info(@$data) {
    
        my $converted = $self->prettySize( 'size' => $info->{'bkp_size'} );
        
        $bkpTbl->row(
                        $info->{'relfilename'},
                        $info->{'start_time'},
                        $info->{'uuid'},
                        $converted,
                        $info->{'partial'},
                        $info->{'incremental'},
                        $info->{'compact'},
                        $info->{'compressed'}
                    );
        $bkpTbl->hr;
    
        $sum += $info->{'bkp_size'};
        
    } # for

    my $convSum = $self->prettySize( 'size' => $sum );
    
    $bkpTbl->row('','','',$convSum,'','','','');
    $bkpTbl->hr;
    
    print $bkpTbl->draw;

} # end sub tbl

=item C<lst>

Method outputs data passed in list format

param:

    data array_ref - list of hashes with information
    
return:

    void
    
=cut

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

=item C<getGlacierBackupsInfo>

Gets glacier and history information for all backups or for specific backup,
identified by uuid

param:

    uuid string - uuid which identifies file
    
return:

    $backupsInfo array_ref - array of hash_refs containing info about backup/s
    
=cut

sub getGlacierBackupsInfo {

    my $self = shift;
    my %params = @_;
    my $uuid = $params{'uuid'};
    my @backupsInfo = ();

    $self->log('debug')->debug("Getting remote backups info with params: ", , sub { Dumper(\%params) });
   
    my $deletedQuery = "SELECT journal.glacier_id FROM journal";
    $deletedQuery .= " WHERE type='DELETED'";
    
    my $getQuery = "SELECT * FROM journal JOIN glacier ON";
    $getQuery .= " journal.glacier_id = glacier.glacier_id WHERE";
    $getQuery .= " journal.glacier_id NOT IN (" . $deletedQuery . ") AND type='CREATED'";
    
    if( $uuid ) {
        $getQuery .= " AND glacier.uuid='" . $uuid . "'";
    } # if
    
    $self->log('debug')->debug("Query: ", $getQuery);
    
    try {
        @backupsInfo = @{ $self->localDbh->selectall_arrayref($getQuery, { Slice => {} }) };
    } catch {
        my $error = @_ || $_;
        $self->log('base')->error("Error: ", $error, " Query: " . $getQuery);
        croak "Error: " . $error;
    }; # try
    
    return \@backupsInfo;

} # end sub getRmtBackupsInfo

=item C<getGlacierBackupChain>

Gets backup chain for backup, if it is incremental it gets all backups till full

param:

    uuid string - uuid which identifies file/backup
    
return:

    $chain array_ref - list of hash_refs containing info about backups
    
=cut

sub getGlacierBackupChain {

    my $self            = shift;
    my %params          = @_;
    my $uuid            = $params{'uuid'};
 
    my $histIdQuery = "SELECT parent_id FROM glacier WHERE uuid='" . $uuid . "'";
    my $timeQuery = "SELECT start_time FROM glacier WHERE uuid='" . $uuid . "'";
    
    my $query = "SELECT * FROM journal JOIN glacier ON journal.glacier_id = glacier.glacier_id";
    $query .= " WHERE journal.glacier_id NOT IN";
    $query .= " (SELECT journal.glacier_id FROM journal WHERE type='DELETED')";
    $query .= " AND type='CREATED' AND (glacier.history_id IN";
    $query .= " (" . $histIdQuery . ") OR parent_id IN (" . $histIdQuery . ")";
    $query .= " AND start_time <= (" . $timeQuery . "))";
    $query .= " ORDER BY innodb_to_lsn ASC, start_time ASC";
    
    $self->log('debug')->debug("Query: ", $query);
    
    my @chain = ();
    
    try {
        @chain = @{ $self->localDbh->selectall_arrayref($query, { Slice => {} }) };
    } catch {
        my $error = @_ || $_;
        $self->log('base')->error("Error: ", $error, " Query: " . $query);
        croak "Error: " . $error;
    };
    
    $self->log('debug')->debug("Dumping backup chain: ", sub { Dumper(@chain) });
    
    return \@chain;

} # end sub getGlacierBackupChain

=item C<calcPartSize>

Method accepts size and calculates what should be part size for upload to
amazon glacier, according AWS documentation maximum can be 4G part and maximun
number of them per archive 10000, thus max. 4G * 10000 = 40TB file size. We
are calculating how much percent is passed size from 40TB and then use as percent 
from maximum part size (4G) to get part size in megabytes (must be multiple of 1024)
and greater than 1MB (according AWS docs), then we convert this part size in
MB to bytes (AWS accepts part size in bytes)

param:

    size integer - number of bytes, file size
    
return:

    $ceiledPartSize integer - number of bytes, size which should be used
                              as size of upload part in multipart glacier upload
  
=back

=cut

sub calcPartSize {

    my $self = shift;
    my %params = @_;
    my $fileSize = $params{'size'};
    my $config = $params{'config'};
    
    if( ! defined $config->{'memorycap'} ) {
        $self->log('base')->error("Glacier needs memorycap to be defined in config!");
        croak "Glacier needs memorycap to be defined in config!";
    } # if
       
    my $capMaxPart = $config->{'memorycap'} / $config->{'concurrency'};
    my $capBinMaxPart = sprintf("%b", $capMaxPart);
    my $binNumLength = length($capBinMaxPart);
    my $zeros = "0" x ($binNumLength - 1);
    my $capMinPart= oct("0b1" . $zeros);
    
    # max file size able to upload in glacier is 4Gx10000
    my $maxSize = 40 * 1024 * 1024 * 1024 * 1024;
    my $maxPartSize = 4 * 1024 * 1024 * 1024;
    my $finalPartSize = 0;
    
    if( $fileSize > $maxSize ) {
        $self->log('base')->error("File size is bigger than max size: ", $maxSize);
        croak "File size is bigger than max size: " . $maxSize;
    } # if
    
    my $numFilesCapMinPart = $fileSize / $capMinPart;

    if( $numFilesCapMinPart > 10000 ) {
        $self->log('base')->error("Cannot upload file with this memory cap, too low memory for big file/part size!");
        croak "Cannot upload file with this memory cap, too low memory for big file!";
    } else {
        $finalPartSize = $capMinPart;
    } # if
    
    return $finalPartSize;
    
} # end sub calcPartSize

no Moose::Role;

=head1 AUTHOR

    PAVOL IPOTH <pavol.ipoth@gmail.com>

=head1 COPYRIGHT

    Pavol Ipoth, ALL RIGHTS RESERVED, 2015

=head1 License

    GPLv3

=cut

1;
