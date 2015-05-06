package Backup::Type::Incremental;

use Moose;
use namespace::autoclean;
use Carp;
use Try::Tiny;
use warnings;
use autodie;
use File::Glob;

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

    # we need base directory from which incremental backup will start backing up
    my $lastBackupDir = $self->getLastBackup('hostBkpDir' => $self->{'hostBkpDir'});

    # preparing and executing tool incremental command
    # incremental-force-scan is requisite because backup without scan is
    # implemented only by percona mysql version
    my $fullBkpCmd = "innobackupex --incremental --user=" . $self->{'user'};
    $fullBkpCmd .= " --compress --host=" . $self->{'host'};
    $fullBkpCmd .= " --password=" . $self->{'pass'} . " --incremental-force-scan";
    $fullBkpCmd .= " --incremental-basedir=" . $lastBackupDir . " " . $self->{'hostBkpDir'};

    my $shell = Term::Shell->new();
    my $result = $shell->execCmd('cmd' => $fullBkpCmd, 'cmdsNeeded' => [ 'innobackupex' ]);

    $shell->fatal($result);

} # end sub backup

sub restore() {
}

sub dump() {
}

sub getLastBackup() {

    my $self = shift;
    my %params = @_;
    my $hostBkpDir = $params{'hostBkpDir'};
    my %lsn = ();

    # getting files with lsn number from all backups in host backup directory 
    my @sources = <$hostBkpDir/*/xtrabackup_checkpoints>;

    # collecting lsn numbers
    for my $file(@sources) {

        my $fh = IO::File->new();
        $fh->open("< $file");
        my @lines = <$fh>;
        $fh->close();

        for my $line(@lines) {
            if( $line =~ /last_lsn\s+=\s+(\d+)/) {
                $lsn{$1} = $file;
            } # if
        } # for
 
    } # for

    # sorting and getting highest lsn numbers, returning corresponing backup
    # directory
    my @sortedLsn = sort { $a <=> $b } keys %lsn;

    my $lastBackupLsn = pop @sortedLsn;
    my $lastBkpFile = $lsn{$lastBackupLsn};
    
    $lastBkpFile =~ /(.*)\/.*$/;
    my $lastBkpDir = $1;

    return $lastBkpDir; 

} # end sub getLastBackup

no Moose::Role;

1;