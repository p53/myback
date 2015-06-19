package Backup::Type::Incremental;

use Moose;
use namespace::autoclean;
use Carp;
use Try::Tiny;
use warnings;
use autodie;
use File::Glob;
use File::Copy;
use File::Path;
use Data::Dumper;
use DBI;
use YAML::Tiny;
use File::stat;

use Term::Shell;

with 'Backup::BackupInterface', 'MooseX::Log::Log4perl';

sub backup() {

    my $self = shift;
    my %params = @_;
    my $compSuffix = $self->{'compressions'}->{$self->{'compression'}};
    my $compUtil = $self->{'compression'};
    
    # checking if all needed parameters present
    if( !( defined $params{'user'} && defined $params{'pass'} ) ) {
        $self->log->error("You need to specify user, pass!");
        croak "You need to specify user, pass!";
    } # if

    if( ! -d $params{'hostBkpDir'} ) {
        $self->log->error("$params{'hostBkpDir'} does not exist, incremental backup needs previous backup!");
        croak "$params{'hostBkpDir'} does not exist, incremental backup needs previous backup!";
    } # if

    my $lastBkpInfo = $self->getLastBkpInfo(
                                                'user' => $params{'user'},
                                                'pass' => $params{'pass'},
                                                'socket' => $params{'socket'}
                                            );

    $self->log('debug')->debug("Dumping last backup info before backup: ", sub { Dumper($lastBkpInfo) });

    my $dateTime = DateTime->now();
    my $now = $dateTime->ymd('-') . 'T' . $dateTime->hms('-');
    my $bkpDir = $params{'hostBkpDir'} . "/" . $now;

    $self->log('base')->info("Creating backup directory for local backup:", $bkpDir);

    mkpath($bkpDir) if ! -d $bkpDir;

    my $bkpFileName = $bkpDir . "/" . $now . ".xb." . $compSuffix;

    # preparing and executing tool incremental command
    # incremental-force-scan is requisite because backup without scan is
    # implemented only in versions higher than 5.1
    my $bkpCmd = "innobackupex --incremental --user=" . $params{'user'};
    $bkpCmd .= " --history --stream=xbstream --host=" . $params{'host'};
    $bkpCmd .= " --password='$params{'pass'}' --incremental-force-scan";
    $bkpCmd .= " --incremental-history-uuid=" . $lastBkpInfo->{'uuid'};
    $bkpCmd .= " --socket=" . $params{'socket'};
    $bkpCmd .= " " . $params{'hostBkpDir'};
    $bkpCmd .= "| " . $compUtil . " > " . $bkpFileName;

    $self->log('base')->info("Backing up");

    my $shell = Term::Shell->new();
    my $result = '';

    try{
        $result = $shell->execCmd('cmd' => $bkpCmd, 'cmdsNeeded' => [ 'innobackupex', $compUtil ]);
        $shell->fatal($result);
    } catch {
        File::Path::remove_tree($bkpDir . "/" . $now);
        $self->log->error("Shell command failed! Message: ", $result->{'msg'});
        croak "Shell command failed! Message: " . $result->{'msg'};
    }; # try
    
    $lastBkpInfo = $self->getLastBkpInfo(
                                            'user' => $params{'user'},
                                            'pass' => $params{'pass'},
                                            'socket' => $params{'socket'}
                                        );

    $self->log('debug')->debug("Dumping last backup info after backup: ", sub { Dumper($lastBkpInfo) });

    my $uuidFileName = $bkpDir . "/" . $lastBkpInfo->{'uuid'} . ".xb." . $compSuffix;
    my $uuidConfFile = $bkpDir . "/" . $lastBkpInfo->{'uuid'} . ".yaml";

    $self->log('base')->info("Renaming $bkpFileName to $uuidFileName");

    move($bkpFileName, $uuidFileName);
    
    my $filesize = stat($uuidFileName)->size;

    $lastBkpInfo = $self->bkpInfoTimeToUTC('bkpInfo' => $lastBkpInfo);
    $lastBkpInfo->{'bkp_size'} = $filesize;
    
    $self->log('debug')->debug("Dumping last backup info with UTC times: ", sub { Dumper($lastBkpInfo) });
    $self->log('base')->info("Writing YAML config for remote backups");

    my $yaml = YAML::Tiny->new($lastBkpInfo);
    $yaml->write($uuidConfFile);

    $self->log('base')->info("Local backup finished!");

} # end sub backup

sub restore() {

    my $self = shift;
    my %params = @_;
    my $uuid = $params{'uuid'};
    my $restoreLocation = $params{'location'};
    my $backupsInfo = $params{'backupsInfo'};
    my $currentBkp = $backupsInfo->{$uuid};
    my $compSuffix = $self->{'compressions'}->{$self->{'compression'}};
    my $compUtil = $self->{'compression'};
    my $result = {};
   
    if( ! -d $restoreLocation ) {
        $self->log('base')->info("Creating restore directory $restoreLocation");
        mkdir $restoreLocation;
    } # if

    my $chain = [];

    $self->log('base')->info("Getting backups info till nearest previous full backup");

    $chain = $self->getBackupChain(
                                    'backupsInfo' => $backupsInfo, 
                                    'uuid' => $uuid, 
                                    'chain' => $chain
                                );

    $self->log('debug')->debug("Dumping backups chain:", sub { Dumper($chain) });

    my @revChain = reverse @$chain;                            
    my $fullBkp = shift @revChain;

    $self->log('base')->info("Creating restore directory $restoreLocation");

    if( ! -d $restoreLocation ) {
        $self->log('base')->info("Creating restore directory $restoreLocation");
        mkpath($restoreLocation);
    } # if

    my @files = glob($params{'hostBkpDir'} . "/*/" . $fullBkp->{'uuid'} . ".xb." . $compSuffix);
    my $bkpFile = $files[0];

    if( ! -f $bkpFile ) {
        $self->log->error("Cannot find file with uuid " . $fullBkp->{'uuid'} . "!");
        croak "Cannot find file with uuid " . $fullBkp->{'uuid'} . "!";
    } # if
    
    $self->log('base')->info("Decompressing full backup $bkpFile to $restoreLocation");

    my $shell = Term::Shell->new();

    my $decompCmd = $compUtil . " -c -d " . $bkpFile . "|xbstream -x -C " . $restoreLocation;

    $result = $shell->execCmd('cmd' => $decompCmd, 'cmdsNeeded' => [ $compUtil, 'xbstream' ]);

    $shell->fatal($result);

    $self->log('base')->info("Applying innodb logs on full backup");

    my $restoreFullCmd = "innobackupex --apply-log --redo-only " . $restoreLocation;

    try{
        $result = $shell->execCmd('cmd' => $restoreFullCmd, 'cmdsNeeded' => [ 'innobackupex' ]);
    } catch {
        $self->log->error("Error: ", $result->{'msg'});
        remove_tree($restoreLocation);
        $shell->fatal($result);
    }; # try

    $self->log('base')->info("Restoring each previous incremental backup and applying innodb logs");

    my $restoreIncrCmd = "innobackupex --apply-log --redo-only " . $restoreLocation . " --incremental-dir=";

    for my $prevBkp(@revChain) {

        my @files = glob($params{'hostBkpDir'} . "/*/" . $prevBkp->{'uuid'} . ".xb." . $compSuffix);
        my $bkpFile = $files[0];
        
        if( ! -f $bkpFile ) {
            $self->log->error("Cannot find file with uuid " . $prevBkp->{'uuid'} . "!");
            croak "Cannot find file with uuid " . $prevBkp->{'uuid'} . "!";
        } # if
        
        $bkpFile =~ /(.*)\/(.*)$/;

        $self->log('base')->info("Incremental backup in dir $1 and uuid" . $prevBkp->{'uuid'});

        $restoreIncrCmd .= $1;
        
        try{
            $result = $shell->execCmd('cmd' => $restoreIncrCmd, 'cmdsNeeded' => [ 'innobackupex' ]);
        } catch {
            $self->log->error("Error: ", $result->{'msg'});
            remove_tree($restoreLocation);
            $shell->fatal($result);
        }; # try

    } # for

    my $lastIncrCmd = "innobackupex --apply-log " . $restoreLocation . " --incremental-dir=";

    @files = glob($params{'hostBkpDir'} . "/*/" . $currentBkp->{'uuid'} . ".xb." . $compSuffix);
    $bkpFile = $files[0];
    
    if( ! -f $bkpFile ) {
        $self->log->error("Cannot find file with uuid " . $currentBkp->{'uuid'} . "!");
        croak "Cannot find file with uuid " . $currentBkp->{'uuid'} . "!";
    } # if
        
    $bkpFile =~ /(.*)\/(.*)$/;

    $self->log('base')->info("Restoring last incremental backup, applying innodb logs and reverting uncommited transactions");

    $lastIncrCmd .= $1;

    try{
        $result = $shell->execCmd('cmd' => $lastIncrCmd, 'cmdsNeeded' => [ 'innobackupex' ]);
    } catch {
        $self->log->error("Error: ", $result->{'msg'});
        remove_tree($restoreLocation);
        $shell->fatal($result);
    }; # try

    my $restoreFullRollbackCmd = "innobackupex --apply-log " . $restoreLocation;

    try{
        $result = $shell->execCmd('cmd' => $restoreFullRollbackCmd, 'cmdsNeeded' => [ 'innobackupex' ]);
    } catch {
        $self->log->error("Error: ", $result->{'msg'});
        remove_tree($restoreLocation);
        $shell->fatal($result);
    }; # try

    $self->log('base')->info("Removing percona files in $restoreLocation");

    unlink glob("$restoreLocation/xtrabackup_*");
    #unlink "$restoreLocation/backup-my.cnf";

    $self->log('base')->info("Restoration successful");

} # end sub restore

sub rmt_backup() {

    my $self = shift;
    my %params = @_;
    my $hostInfo = $params{'hostInfo'};
    my $privKeyPath = $params{'privKeyPath'};
    my $bkpFileName = $params{'bkpFileName'};
    my $compUtil = $self->{'compression'};
    
    my $shell = Term::Shell->new();
    
    $self->log('base')->info("Checking if history exists on remote host: ", $hostInfo->{'ip'});
    
    my $checkIfDbExists = "ssh -i " . $privKeyPath . " " . $hostInfo->{'ip'} . " '";
    $checkIfDbExists .= 'mysql -e "SELECT COUNT(*) AS dbexists from';
    $checkIfDbExists .= ' information_schema.tables WHERE table_schema=\"PERCONA_SCHEMA\"';
    $checkIfDbExists .= ' and table_name=\"xtrabackup_history\""';
    $checkIfDbExists .= ' -u ' . $hostInfo->{'user'} . " -p\Q$hostInfo->{'pass'}\E";
    $checkIfDbExists .= ' -h ' . $hostInfo->{'local_host'} . ' -X';
    $checkIfDbExists .= ' -S ' . $hostInfo->{'socket'} . "'";
    
    my $result = $shell->execCmd('cmd' => $checkIfDbExists, 'cmdsNeeded' => [ 'ssh' ]);

    my $dbExists = $self->mysqlXmlToHash('xml' => $result->{'msg'});
    
    $self->log('debug')->debug("Result of history check is: ", $result->{'msg'});
    
    if($dbExists->{'dbexists'} == 0 ) {
        $self->log->error("Incremental backup needs previous backup, no local database found on remote host!");
        croak "Incremental backup needs previous backup, no local database found on remote host!";
    } # if
    
    $self->log('base')->info("Getting info about last backup for host: ", $hostInfo->{'ip'});
    
    my $lastBkpInfoCmd = "ssh -i " . $privKeyPath . " " . $hostInfo->{'ip'} . " '";
    $lastBkpInfoCmd .= 'mysql -e "select * from PERCONA_SCHEMA.xtrabackup_history';
    $lastBkpInfoCmd .= ' ORDER BY innodb_to_lsn DESC, start_time DESC LIMIT 1"';
    $lastBkpInfoCmd .= ' -u ' . $hostInfo->{'user'} . " -p\Q$hostInfo->{'pass'}\E";
    $lastBkpInfoCmd .= ' -h ' . $hostInfo->{'local_host'} . ' -X';
    $lastBkpInfoCmd .= ' -S ' . $hostInfo->{'socket'} . "'";
    
    $result = $shell->execCmd('cmd' => $lastBkpInfoCmd, 'cmdsNeeded' => [ 'ssh' ]);

    $shell->fatal($result);
    
    my $lastBkpInfo = $self->mysqlXmlToHash('xml' => $result->{'msg'});
    
    $self->log('debug')->debug("Dumping last backup info: ", sub { Dumper($lastBkpInfo) });
    
    $self->log('base')->info("Executing incremental backup on remote host $hostInfo->{'ip'} on socket $hostInfo->{'socket'}");
    
    my $rmtBkpCmd = "ssh -i " . $privKeyPath . " " . $hostInfo->{'ip'} . " '";
    $rmtBkpCmd .= "innobackupex --incremental --user=" . $hostInfo->{'user'};
    $rmtBkpCmd .= " --history --stream=xbstream --host=" . $hostInfo->{'local_host'};
    $rmtBkpCmd .= " --password=\Q$hostInfo->{'pass'}\E --incremental-force-scan";
    $rmtBkpCmd .= " --incremental-history-uuid=" . $lastBkpInfo->{'uuid'};
    $rmtBkpCmd .= " --socket=" . $hostInfo->{'socket'};
    $rmtBkpCmd .= " " . $hostInfo->{'local_dir'};
    $rmtBkpCmd .= "| " . $compUtil . " -c ' > " . $bkpFileName;
    
    $result = $shell->execCmd('cmd' => $rmtBkpCmd, 'cmdsNeeded' => [ 'ssh' ]);

    $self->log('debug')->debug("Result of command is: ", $result->{'msg'});
    
    $shell->fatal($result);
 
    return $result;
    
} # end sub rmt_backup

sub restore_rmt() {

    my $self = shift;
    my %params = @_;

    $self->restore(%params);

} # end sub restore_rmt

sub getBackupChain() {

    my $self = shift;
    my %params = @_;
    my $backupsInfo = $params{'backupsInfo'};
    my $uuid = $params{'uuid'};
    my $chain = $params{'chain'};
    my $currentBackup = $backupsInfo->{$uuid};

    delete $backupsInfo->{$uuid};

    my @candidates = ();
    my $closestCandidate = {};

    for my $backup(values %$backupsInfo) {    

        if( $backup->{'innodb_to_lsn'} == $currentBackup->{'innodb_from_lsn'} ) {
            push(@candidates, $backup);
        } # if

    } # for

    if( length(@candidates) > 1 ) {
        
        my %timeDiffs = ();
        
        for my $candidate(@candidates) {
            my $diff = $currentBackup->{'start_unix_time'} - $candidate->{'start_unix_time'};
            $timeDiffs{$diff} = $candidate;
        } # for

        my @sortedDiffs = sort{ $a <=> $b } keys %timeDiffs;
        my $minDiff = $sortedDiffs[length(@sortedDiffs) - 1];

        $closestCandidate = $timeDiffs{$minDiff};

    } else {
        $closestCandidate = $candidates[0];
    } # if

    push(@$chain, $closestCandidate);

    if( $closestCandidate->{'incremental'} eq 'Y' ) {
        $self->getBackupChain(
                                'backupsInfo' => $backupsInfo, 
                                'uuid' => $closestCandidate->{'uuid'}, 
                                'chain' => $chain
                            );
    } # if

    return $chain;

} # end sub getBackupChain

no Moose::Role;

1;