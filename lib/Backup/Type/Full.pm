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
use DBI;
use YAML::Tiny;

use Term::Shell;

with 'Backup::BackupInterface';

sub backup() {

    my $self = shift;
    my %params = @_;

    if( !( defined $params{'user'} && defined $params{'pass'} ) ) {
        croak "You need to specify user, pass!";
    } # if

    my $dateTime = DateTime->now();
    my $now = $dateTime->ymd('-') . 'T' . $dateTime->hms('-');
    my $bkpDir = $self->{'hostBkpDir'} . "/" . $now;

    mkpath($bkpDir) if ! -d $bkpDir;

    my $bkpFileName = $bkpDir . "/" . $now . ".xb.bz2";

    my $fullBkpCmd = "innobackupex --user=" . $self->{'user'};
    $fullBkpCmd .= " --history --stream=xbstream --host=" . $self->{'host'};
    $fullBkpCmd .= " --password=" . $self->{'pass'} . " " . $self->{'hostBkpDir'};
    $fullBkpCmd .= " --socket=" . $self->{'socket'};
    $fullBkpCmd .= "|bzip2 > " . $bkpFileName;

    my $shell = Term::Shell->new();
    my $result = $shell->execCmd('cmd' => $fullBkpCmd, 'cmdsNeeded' => [ 'innobackupex', 'bzip2' ]);

    $shell->fatal($result);

    my $lastBkpInfo = $self->getLastBkpInfo();

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

    my $result = {};

    if( -d $restoreLocation ) {
        croak "Restore location already exists!";
    } # if

    mkdir $restoreLocation;

    my @files = glob($self->{'hostBkpDir'} . "/*/" . $uuid . ".xb.bz2");
    my $bkpFile = $files[0];

    my $shell = Term::Shell->new();

    my $decompCmd = "bzip2 -c -d " . $bkpFile . "|xbstream -x -C " . $restoreLocation;

    $result = $shell->execCmd('cmd' => $decompCmd, 'cmdsNeeded' => [ 'bzip2', 'xbstream' ]);

    $shell->fatal($result);

    my $restoreCmd = "innobackupex --apply-log " . $restoreLocation;

    try{
        $result = $shell->execCmd('cmd' => $restoreCmd, 'cmdsNeeded' => [ 'innobackupex' ]);
    } catch {
        remove_tree($restoreLocation);
        $shell->fatal($result);
    }; # try

    unlink glob("$restoreLocation/xtrabackup_*");
    unlink "$restoreLocation/backup-my.cnf";

} # end sub restore

sub dump() {
}

no Moose::Role;

1;