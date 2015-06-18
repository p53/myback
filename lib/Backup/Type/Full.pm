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
use DateTime;
use Data::Dumper;
use DBI;
use YAML::Tiny;

use Term::Shell;

with 'Backup::BackupInterface', 'MooseX::Log::Log4perl';

sub backup() {

    my $self = shift;
    my %params = @_;
    my $compSuffix = $self->{'compressions'}->{$self->{'compression'}};
    my $compUtil = $self->{'compression'};
    
    if( !( defined $params{'user'} && defined $params{'pass'} ) ) {
        $self->log->error("You need to specify user, pass!");
        croak "You need to specify user, pass!";
    } # if

    my $dateTime = DateTime->now();
    my $now = $dateTime->ymd('-') . 'T' . $dateTime->hms('-');
    my $bkpDir = $params{'hostBkpDir'} . "/" . $now;

    $self->log('base')->info("Creating backup directory for local backup:", $bkpDir);

    mkpath($bkpDir) if ! -d $bkpDir;  
    
    my $bkpFileName = $bkpDir . "/" . $now . ".xb." . $compSuffix;

    my $fullBkpCmd = "innobackupex --user=" . $params{'user'};
    $fullBkpCmd .= " --history --stream=xbstream --host=" . $params{'host'};
    $fullBkpCmd .= " --password='$params{'pass'}' " . $params{'hostBkpDir'};
    $fullBkpCmd .= " --socket=" . $params{'socket'};
    $fullBkpCmd .= "| " . $compUtil . " > " . $bkpFileName;

    $self->log('base')->info("Backing up");

    my $shell = Term::Shell->new();
    my $result = $shell->execCmd('cmd' => $fullBkpCmd, 'cmdsNeeded' => [ 'innobackupex', $compUtil ]);

    $shell->fatal($result);

    $self->log('base')->info("Full backup of host $params{'host'} to $params{'hostBkpDir'} on socket $params{'socket'} to file $bkpFileName successful");

    my $lastBkpInfo = $self->getLastBkpInfo(
                                                'user' => $params{'user'},
                                                'pass' => $params{'pass'},
                                                'socket' => $params{'socket'}
                                            );

    $self->log('debug')->debug("Dumping last backup info: ", sub { Dumper($lastBkpInfo) });

    my $uuidFileName = $bkpDir . "/" . $lastBkpInfo->{'uuid'} . ".xb." . $compSuffix;
    my $uuidConfFile = $bkpDir . "/" . $lastBkpInfo->{'uuid'} . ".yaml";

    $self->log('base')->info("Renaming $bkpFileName to $uuidFileName");

    move($bkpFileName, $uuidFileName);

    $lastBkpInfo = $self->bkpInfoTimeToUTC('bkpInfo' => $lastBkpInfo);

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
    my $compSuffix = $self->{'compressions'}->{$self->{'compression'}};
    my $compUtil = $self->{'compression'};
    my $result = {};

    if( ! -d $restoreLocation ) {
        $self->log('base')->info("Creating restore directory $restoreLocation");
        mkpath($restoreLocation);
    } # if

    my @files = glob($params{'hostBkpDir'} . "/*/" . $uuid . ".xb." . $compSuffix);
    my $bkpFile = $files[0];

    if( ! -f $bkpFile ) {
        $self->log->error("Cannot find file with uuid $uuid!");
        croak "Cannot find file with uuid $uuid!";
    } # if
    
    $self->log('base')->info("Decompressing backup $bkpFile to $restoreLocation");

    my $shell = Term::Shell->new();

    my $decompCmd = $compUtil . " -c -d " . $bkpFile . " | xbstream -x -C " . $restoreLocation;

    $result = $shell->execCmd('cmd' => $decompCmd, 'cmdsNeeded' => [ $compUtil, 'xbstream' ]);

    $shell->fatal($result);

    $self->log('base')->info("Applying innodb log and reverting uncommitted transactions to $restoreLocation");

    my $restoreCmd = "innobackupex --apply-log " . $restoreLocation;

    try{
        $result = $shell->execCmd('cmd' => $restoreCmd, 'cmdsNeeded' => [ 'innobackupex' ]);
    } catch {
        $self->log->error("Error: ", $result->{'msg'});
        remove_tree($restoreLocation);
        $shell->fatal($result);
    }; # try

    $self->log('base')->info("Removing percona files in $restoreLocation");

    #unlink glob("$restoreLocation/xtrabackup_*");
    unlink "$restoreLocation/backup-my.cnf";

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
    
    $self->log('base')->info("Executing full backup on remote host $hostInfo->{'ip'} on socket $hostInfo->{'socket'}");
    
    my $rmtBkpCmd = "ssh -i " . $privKeyPath . " " . $hostInfo->{'ip'} . " '";
    $rmtBkpCmd .= "innobackupex --user=" . $hostInfo->{'user'};
    $rmtBkpCmd .= " --history --stream=xbstream --host=" . $hostInfo->{'local_host'};
    $rmtBkpCmd .= " --password=\Q$hostInfo->{'pass'}\E " . $hostInfo->{'local_dir'};
    $rmtBkpCmd .= " --socket=" . $hostInfo->{'socket'};
    $rmtBkpCmd .= " 2>/dev/null | " . $compUtil . " -c ' > " . $bkpFileName;
    
    my $result = $shell->execCmd('cmd' => $rmtBkpCmd, 'cmdsNeeded' => [ 'ssh' ]);

    $self->log('debug')->debug("Result of command is: ", $result->{'msg'});
    
    $shell->fatal($result);
 
    return $result;
    
} # end sub rmt_backup

sub restore_rmt() {

    my $self = shift;
    my %params = @_;

    $self->restore(%params);

} # end sub restore_rmt

no Moose::Role;

1;