package Backup::Type::Full;

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
use File::Copy::Recursive;
use DateTime;
use Archive::Tar;

use Term::Shell;

with 'Backup::BackupInterface';

sub backup() {

    my $self = shift;
    my %params = @_;

    if( !( defined $params{'user'} && defined $params{'port'} && defined $params{'pass'} ) ) {
        croak "You need to specify user, port, pass!";
    } # if

    my $dateTime = DateTime->now();
    my $now = $dateTime->ymd('-') . 'T' . $dateTime->hms('-');
    my $bkpDir = $self->{'hostBkpDir'} . "/" . $now;

    mkpath($bkpDir) if ! -d $bkpDir;

    my $bkpFileName = $bkpDir . "/" . $now . ".xb.bz2";

    my $fullBkpCmd = "innobackupex --user=" . $self->{'user'};
    $fullBkpCmd .= " --history --stream=xbstream --host=" . $self->{'host'};
    $fullBkpCmd .= " --password=" . $self->{'pass'} . " " . $self->{'hostBkpDir'};
    $fullBkpCmd .= "|bzip2 > " . $bkpFileName;

    my $shell = Term::Shell->new();
    my $result = $shell->execCmd('cmd' => $fullBkpCmd, 'cmdsNeeded' => [ 'innobackupex', 'bzip2' ]);

    $shell->fatal($result);

} # end sub backup

sub restore() {

    my $self = shift;
    my %params = @_;
    my $uuid = $params{'uuid'};
    my $restoreLocation = $params{'location'};
    my $backupsInfo = $params{'backupsInfo'};
    my $result = {};

    if( -d $restoreLocation ) {
        croak "Restore location already exists!";
    } # if

    mkdir $restoreLocation;

    File::Copy::Recursive::dircopy($backupsInfo->{$uuid}->{'bkpDir'}, $restoreLocation);

    my $restoreCmd = "innobackupex --apply-log " . $restoreLocation;

    my $shell = Term::Shell->new();
    
    try{
        $result = $shell->execCmd('cmd' => $restoreCmd, 'cmdsNeeded' => [ 'innobackupex' ]);
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

no Moose::Role;

1;