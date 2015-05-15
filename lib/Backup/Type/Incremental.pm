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
use DBI;
use YAML::Tiny;

use Term::Shell;

with 'Backup::BackupInterface';

sub backup() {

    my $self = shift;
    my %params = @_;

    # checking if all needed parameters present
    if( !( defined $params{'user'} && defined $params{'pass'} ) ) {
        croak "You need to specify user, pass!";
    } # if

    if( ! -d $self->{'hostBkpDir'} ) {
        croak "$self->{'hostBkpDir'} does not exist, incremental backup needs previous backup!";
    } # if

    my $lastBkpInfo = $self->getLastBkpInfo();

    my $dateTime = DateTime->now();
    my $now = $dateTime->ymd('-') . 'T' . $dateTime->hms('-');
    my $bkpDir = $self->{'hostBkpDir'} . "/" . $now;

    mkpath($bkpDir) if ! -d $bkpDir;

    my $bkpFileName = $bkpDir . "/" . $now . ".xb.bz2";

    # preparing and executing tool incremental command
    # incremental-force-scan is requisite because backup without scan is
    # implemented only by percona mysql version
    my $bkpCmd = "innobackupex --incremental --user=" . $self->{'user'};
    $bkpCmd .= " --history --stream=xbstream --host=" . $self->{'host'};
    $bkpCmd .= " --password=" . $self->{'pass'} . " --incremental-force-scan";
    $bkpCmd .= " --incremental-history-uuid=" . $lastBkpInfo->{'uuid'};
    $bkpCmd .= " --socket=" . $self->{'socket'};
    $bkpCmd .= " " . $self->{'hostBkpDir'};
    $bkpCmd .= "|bzip2 > " . $bkpFileName;

    my $shell = Term::Shell->new();
    my $result = $shell->execCmd('cmd' => $bkpCmd, 'cmdsNeeded' => [ 'innobackupex', 'bzip2' ]);

    $shell->fatal($result);

    $lastBkpInfo = $self->getLastBkpInfo();

    my $uuidFileName = $bkpDir . "/" . $lastBkpInfo->{'uuid'} . ".xb.bz2";
    my $uuidConfFile = $bkpDir . "/" . $lastBkpInfo->{'uuid'} . ".yaml";

    move($bkpFileName, $uuidFileName);

    $lastBkpInfo->{'start_time'} =~ /(\d{4})-(\d{2})-(\d{2})\s(\d{2})\:(\d{2})\:(\d{2})/;

    my $startDt = DateTime->new(
        'year' => $1,
        'month' => $2,
        'day' => $3,
        'hour' => $4,
        'minute' => $5,
        'second' => $6,
        'time_zone' => 'local'
    );

    $startDt->set_time_zone('UTC');

    $lastBkpInfo->{'start_time'} = $startDt->ymd('-') . ' ' . $startDt->hms(':');

    $lastBkpInfo->{'end_time'} =~ /(\d{4})-(\d{2})-(\d{2})\s(\d{2})\:(\d{2})\:(\d{2})/;

    my $endDt = DateTime->new(
        'year' => $1,
        'month' => $2,
        'day' => $3,
        'hour' => $4,
        'minute' => $5,
        'second' => $6,
        'time_zone' => 'local'
    );

    $endDt->set_time_zone('UTC');

    $lastBkpInfo->{'end_time'} = $endDt->ymd('-') . ' ' . $endDt->hms(':');

    my $yaml = YAML::Tiny->new($lastBkpInfo);
    $yaml->write($uuidConfFile);

} # end sub backup

sub restore() {

    my $self = shift;
    my %params = @_;
    my $uuid = $params{'uuid'};
    my $restoreLocation = $params{'location'};
    my $backupsInfo = $params{'backupsInfo'};
    my $currentBkp = $backupsInfo->{$uuid};
    my $result = {};
   
    if( -d $restoreLocation ) {
        croak "Restore location already exists!";
    } # if

    my $chain = [];

    $chain = $self->getBackupChain(
                                    'backupsInfo' => $backupsInfo, 
                                    'uuid' => $uuid, 
                                    'chain' => $chain
                                );

    my @revChain = reverse @$chain;                            
    my $fullBkp = shift @revChain;

    mkdir $restoreLocation;

    my @files = glob($self->{'hostBkpDir'} . "/*/" . $fullBkp->{'uuid'} . ".xb.bz2");
    my $bkpFile = $files[0];

    my $shell = Term::Shell->new();

    my $decompCmd = "bzip2 -c -d " . $bkpFile . "|xbstream -x -C " . $restoreLocation;

    $result = $shell->execCmd('cmd' => $decompCmd, 'cmdsNeeded' => [ 'bzip2', 'xbstream' ]);

    $shell->fatal($result);

    my $restoreFullCmd = "innobackupex --apply-log --redo-only " . $restoreLocation;

    try{
        $result = $shell->execCmd('cmd' => $restoreFullCmd, 'cmdsNeeded' => [ 'innobackupex' ]);
    } catch {
        remove_tree($restoreLocation);
        $shell->fatal($result);
    }; # try

    my $restoreIncrCmd = "innobackupex --apply-log --redo-only " . $restoreLocation . " --incremental-dir=";

    for my $prevBkp(@revChain) {

        my @files = glob($self->{'hostBkpDir'} . "/*/" . $prevBkp->{'uuid'} . ".xb.bz2");
        my $bkpFile = $files[0];
        $bkpFile =~ /(.*)\/(.*)$/;

        $restoreIncrCmd .= $1;

        try{
            $result = $shell->execCmd('cmd' => $restoreIncrCmd, 'cmdsNeeded' => [ 'innobackupex' ]);
        } catch {
            remove_tree($restoreLocation);
            $shell->fatal($result);
        }; # try

    } # for

    my $lastIncrCmd = "innobackupex --apply-log " . $restoreLocation . " --incremental-dir=";

    @files = glob($self->{'hostBkpDir'} . "/*/" . $currentBkp->{'uuid'} . ".xb.bz2");
    $bkpFile = $files[0];
    $bkpFile =~ /(.*)\/(.*)$/;

    $lastIncrCmd .= $1;

    try{
        $result = $shell->execCmd('cmd' => $lastIncrCmd, 'cmdsNeeded' => [ 'innobackupex' ]);
    } catch {
        remove_tree($restoreLocation);
        $shell->fatal($result);
    }; # try

    my $restoreFullRollbackCmd = "innobackupex --apply-log " . $restoreLocation;

    try{
        $result = $shell->execCmd('cmd' => $restoreFullRollbackCmd, 'cmdsNeeded' => [ 'innobackupex' ]);
    } catch {
        remove_tree($restoreLocation);
        $shell->fatal($result);
    }; # try

    unlink glob("$restoreLocation/xtrabackup_*");
    unlink "$restoreLocation/backup-my.cnf";

} # end sub restore

sub dump() {
}

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