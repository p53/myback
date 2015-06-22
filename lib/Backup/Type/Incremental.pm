package Backup::Type::Incremental;

=head1 NAME

    Backup::Type::Incremental - one of backup types, serves for executing operations
                         on/with backups

=head1 SYNOPSIS

    my $backupTypeObj = Backup::Type::Incremental->new();
    
    $backupTypeObj->backup(     
                            'bkpType' => 'incremental'
                            'user' => 'backuper',
                            'host' => 'host1',
                            'bkpDir' => '/backups',
                            'hostBkpDir' => '/backups/host1'
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
use Data::Dumper;
use DBI;
use YAML::Tiny;
use File::stat;

use Term::Shell;

with 'Backup::BackupInterface', 'MooseX::Log::Log4perl';

=head1 METHODS

=over 12

=item C<backup>

Method creates incremental backup on local host

param:

    user string - mysql user for local mysql database with bkp history
    pass string - mysql password for mysql user
    socket string - path to local mysql server
    host string - host name of local mysql server
    hostBkpDir string - directory where backup will be stored on local host
    
return:

    void
    
=cut

sub backup {

    my $self       = shift;
    my %params     = @_;
    my $compSuffix = $self->{'compressions'}->{$self->{'compression'}};
    my $compUtil   = $self->{'compression'};
    
    # checking if all needed parameters present
    if( !( defined $params{'user'} && defined $params{'pass'} ) ) {
        $self->log->error("You need to specify user, pass!");
        croak "You need to specify user, pass!";
    } # if

    if( ! -d $params{'hostBkpDir'} ) {
        $self->log->error("$params{'hostBkpDir'} does not exist, incremental backup needs previous backup!");
        croak "$params{'hostBkpDir'} does not exist, incremental backup needs previous backup!";
    } # if

    # we are getting last backup info from host mysql database, because we need
    # this info to be able to start new incremental backup from that point
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
    # using uuid of last previous backup, this will be start point of our
    # new incremental backup
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
    
    # we are getting info about our new backup, this will be last backup from db
    $lastBkpInfo = $self->getLastBkpInfo(
                                            'user' => $params{'user'},
                                            'pass' => $params{'pass'},
                                            'socket' => $params{'socket'}
                                        );

    $self->log('debug')->debug("Dumping last backup info after backup: ", sub { Dumper($lastBkpInfo) });

    # to be able simply find backup during restore, we give it name with uuid
    my $uuidFileName = $bkpDir . "/" . $lastBkpInfo->{'uuid'} . ".xb." . $compSuffix;
    my $uuidConfFile = $bkpDir . "/" . $lastBkpInfo->{'uuid'} . ".yaml";

    $self->log('base')->info("Renaming $bkpFileName to $uuidFileName");

    move($bkpFileName, $uuidFileName);
    
    my $filesize = stat($uuidFileName)->size;

    # to have all info in one format we convert time to UTC wherever we are doing
    # backups
    $lastBkpInfo = $self->bkpInfoTimeToUTC('bkpInfo' => $lastBkpInfo);
    $lastBkpInfo->{'bkp_size'} = $filesize;
    
    $self->log('debug')->debug("Dumping last backup info with UTC times: ", sub { Dumper($lastBkpInfo) });
    $self->log('base')->info("Writing YAML config for remote backups");

    # storing information about backup also in yml file
    # this is used in rmt_tmp_backup
    my $yaml = YAML::Tiny->new($lastBkpInfo);
    $yaml->write($uuidConfFile);

    $self->log('base')->info("Local backup finished!");

} # end sub backup

=item C<restore>

Restores incremental backup stored local host

param:

    uuid string - uuid of restored backup
    
    location string - where we want to restore backup
    
    hostBkpDir string - where we store backup
    
    backupsInfo hash_ref - list of all backups info related to that host alias
    
return:

    void
    
=cut

sub restore {

    my $self            = shift;
    my %params          = @_;
    my $uuid            = $params{'uuid'};
    my $restoreLocation = $params{'location'};
    my $backupsInfo     = $params{'backupsInfo'};
    my $currentBkp      = $backupsInfo->{$uuid};
    my $compSuffix      = $self->{'compressions'}->{$self->{'compression'}};
    my $compUtil        = $self->{'compression'};
    my $result          = {};
   
    if( ! -d $restoreLocation ) {
        $self->log('base')->info("Creating restore directory $restoreLocation");
        mkdir $restoreLocation;
    } # if

    my $chain = [];

    $self->log('base')->info("Getting backups info till nearest previous full backup");

    # to be able to restore incremental backup, we need previous incremental
    # backups plus full backup and we are getting this info here, backups are
    # returned from newest to oldest
    $chain = $self->getBackupChain(
                                    'backupsInfo' => $backupsInfo, 
                                    'uuid' => $uuid, 
                                    'chain' => $chain
                                );

    $self->log('debug')->debug("Dumping backups chain:", sub { Dumper($chain) });

    # we need to start restore from full backup - oldest first
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

    # applying logs on full backup
    my $restoreFullCmd = "innobackupex --apply-log --redo-only " . $restoreLocation;

    try{
        $result = $shell->execCmd('cmd' => $restoreFullCmd, 'cmdsNeeded' => [ 'innobackupex' ]);
    } catch {
        $self->log->error("Error: ", $result->{'msg'});
        remove_tree($restoreLocation);
        $shell->fatal($result);
    }; # try

    $self->log('base')->info("Restoring each previous incremental backup and applying innodb logs");

    # we apply each incremental backup from oldest to newest on full backup
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

    # applying our last incremental backup plus reverting uncommited transactions
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

    #unlink glob("$restoreLocation/xtrabackup_*");
    # we don't remove this because we might it need for server start
    unlink "$restoreLocation/backup-my.cnf";

    $self->log('base')->info("Restoration successful");

} # end sub restore

=item C<rmt_backup>

param:

    hostInfo hash_ref - hash info about remote host
    
    privKeyPath string - private key for remote backuped host
    
    bkpFileName string - name of backup file to which we should store backup
                         from remote host

return:

    result hash_ref - hash info from executed command with message and return code
    
=cut

sub rmt_backup {

    my $self        = shift;
    my %params      = @_;
    my $hostInfo    = $params{'hostInfo'};
    my $privKeyPath = $params{'privKeyPath'};
    my $bkpFileName = $params{'bkpFileName'};
    my $compUtil    = $self->{'compression'};
    
    my $shell = Term::Shell->new();
    
    $self->log('base')->info("Checking if history exists on remote host: ", $hostInfo->{'ip'});
    
    # we are checking if local mysql backup history exists, if it is e.g. new
    # host where innobackup haven't been run, there won't exist, so to prevent
    # backup failure we are checking it before running innobackupex script
    # we are getting info in xml, it's easier to parse
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
    
    # we are getting info about last backup on remote host, so we now from where
    # start new incremental backup
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

=item C<restore_rmt>

Method for restoring remote host backups, stored on server

param:

    %params - all params required by proxied method
    
return:

    void

=cut

sub restore_rmt {

    my $self    = shift;
    my %params  = @_;

    $self->restore(%params);

} # end sub restore_rmt

=item C<getBackupChain>

Method gets all previous incremental backup info plus full backup for specified
incremental backup, we need this to be able to restore backup

param:

    backupsInfo hash_ref - all backups related to host alias for specified uuid
    
    uuid string - uuid of restored incremental backup
    
    chain array_ref - empty array ref
    
return:

    chain array_ref - all backups info related to our
                      incremental backup ordered from oldest (so full backup first)
                      to last previous backup
    
=back

=cut

sub getBackupChain {

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

    # if we didn't reach full backup we need to find it's parent
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

=head1 AUTHOR

        PAVOL IPOTH <pavol.ipoth@gmail.com>

=head1 COPYRIGHT

        Pavol Ipoth, ALL RIGHTS RESERVED, 2015

=head1 License

        GPLv3

=cut

1;