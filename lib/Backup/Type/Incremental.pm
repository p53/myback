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
use File::Copy::Recursive;
use Archive::Tar;
use DBI;

use Term::Shell;

with 'Backup::BackupInterface';

sub backup() {

    my $self = shift;
    my %params = @_;

    # checking if all needed parameters present
    if( !( defined $params{'user'} && defined $params{'port'} && defined $params{'pass'} ) ) {
        croak "You need to specify user, port, pass!";
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
    $bkpCmd .= " " . $self->{'hostBkpDir'};
    $bkpCmd .= "|bzip2 > " . $bkpFileName;

    my $shell = Term::Shell->new();
    my $result = $shell->execCmd('cmd' => $bkpCmd, 'cmdsNeeded' => [ 'innobackupex', 'bzip2' ]);

    $shell->fatal($result);

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

    try{
        File::Copy::Recursive::dircopy($fullBkp->{'bkpDir'}, $restoreLocation);
    } catch {
        remove_tree($restoreLocation);
        croak "Failed copying dir " . $fullBkp->{'bkpDir'} . "!";
    };

    my $restoreFullCmd = "innobackupex --apply-log --redo-only " . $restoreLocation;

    my $shell = Term::Shell->new();

    try{
        $result = $shell->execCmd('cmd' => $restoreFullCmd, 'cmdsNeeded' => [ 'innobackupex' ]);
    } catch {
        remove_tree($restoreLocation);
        $shell->fatal($result);
    }; # try

    my $restoreIncrCmd = "innobackupex --apply-log --redo-only " . $restoreLocation . " --incremental-dir=";

    for my $prevBkp(@revChain) {

        $restoreIncrCmd .= $prevBkp->{'bkpDir'};

        try{
            $result = $shell->execCmd('cmd' => $restoreIncrCmd, 'cmdsNeeded' => [ 'innobackupex' ]);
        } catch {
            remove_tree($restoreLocation);
            $shell->fatal($result);
        }; # try

    } # for

    my $lastIncrCmd = "innobackupex --apply-log " . $restoreLocation . " --incremental-dir=";
    $lastIncrCmd .= $currentBkp->{'bkpDir'};

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
    unlink glob("$restoreLocation/*.qp");
    unlink "$restoreLocation/backup-my.cnf";

} # end sub restore

sub dump() {
}

sub getLastBkpInfo() {

    my $self = shift;
    my %params = @_;
    my $hostBkpDir = $params{'hostBkpDir'};
    my $info = {};

    my $dbh = DBI->connect(
                            "DBI:mysql:database=PERCONA_SCHEMA;host=localhost",
                            $self->{'user'}, 
                            $self->{'pass'},
                            {'RaiseError' => 1}
                        );

    my $query = "SELECT * FROM PERCONA_SCHEMA.xtrabackup_history";
    $query .= " ORDER BY innodb_to_lsn DESC, start_time DESC LIMIT 1";

    my $sth = $dbh->prepare($query);
    $sth->execute();

    while (my $ref = $sth->fetchrow_hashref) {
        $info = $ref;
    } # while

    $sth->finish;
    $dbh->disconnect();

    return $info; 

} # end sub getLastBackup

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