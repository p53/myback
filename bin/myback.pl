#!/usr/bin/perl -w

use strict;
use warnings;
use autodie;
use Getopt::Long;
use Pod::Usage;
use Sys::Hostname;
use DateTime;
use Data::Dumper;
use Log::Log4perl qw(:levels);
use FindBin;
use lib "$FindBin::RealBin/../lib";

use Backup::Backup;

# configuring our logger, we have two one for debug output
# and one for normal output, cause they have different formats
my $log_conf = "$FindBin::RealBin/../etc/log.conf";
Log::Log4perl::init($log_conf);

my $dbgLogger = Log::Log4perl->get_logger('debug');
my $baseLogger = Log::Log4perl->get_logger('base');

$dbgLogger->level($DEBUG);
$baseLogger->level($INFO);

$dbgLogger->debug('Starting');
$dbgLogger->debug('Parsing arguments');

my $help = 0;
my $action = '';
my $dir = '';
my $host = '';
my $user = '';
my $pass = '';
my $location = '';
my $dbname = '';
my $config = '';
my $type = '';
my $id = '';
my $format = '';
my $socket = '';
my $optimize = '';

my $allowedTypes = {
                        'incremental' => 1,
                        'full' => 1
                    };

my $allowedActions = {
                        'backup' => 1,
                        'restore' => 1,
                        'list' => 1,
                        'rmt_backup' => 1,
                        'list_rmt' => 1,
                        'restore_rmt' => 1,
                        'dump_rmt' => 1
                    };

my $allowedFormats = {
                        'tbl' => 1,
                        'lst' => 1
                    };

my $allowOptimize = {
                        'yes' => 1,
                        'no' => 1
                    };

pod2usage(-verbose => 3) unless @ARGV;

GetOptions(
                'action|a=s' => \$action,
                'dir|d=s' => \$dir,
                'host|h=s' => \$host,
                'type|b:s' => \$type,
                'user|u:s' => \$user,
                'pass|s:s' => \$pass,
                'loc|l:s' => \$location,
                'id|i:s' => \$id,
                'dbname|n:s' => \$dbname,
                'config|c:s' => \$config,
                'format|f:s' => \$format,
                'socket|o:s' => \$socket,
                'optimize|z:s' => \$optimize,
                'help!' => \$help
        ) or pod2usage(-verbose => 3);

pod2usage(-verbose => 3) if $help;
pod2usage(1) if !$action;

pod2usage(-verbose => 3) if !defined( $allowedActions->{$action} );

pod2usage(1) if ( $action eq 'restore' && !($dir && $host && $id && $location && $user && $pass) );
pod2usage(1) if ( $action eq 'list' && !($host && $format && $user && $pass) );
pod2usage(1) if ( $action eq 'list' && !defined( $allowedFormats->{$format} ) );

pod2usage(1) if ( $action eq 'backup' && !defined( $allowedTypes->{$type} ) );
pod2usage(1) if ( $action eq 'backup' && !defined( $allowOptimize->{$optimize} ) );

if( $action eq 'backup' && !($dir && $type && $host && $user && $pass && $optimize) ) {
    pod2usage(1);
} # if

if( $action eq 'dump' && !($dir && $location && $dbname && $host) ) {
    pod2usage(1);
} # if

pod2usage(1) if ( $action eq 'rmt_backup' && !defined( $allowOptimize->{$optimize} ) );
pod2usage(1) if ( $action eq 'rmt_backup' && !($dir && $host && $type && $optimize) );
pod2usage(1) if ( $action eq 'list_rmt' && !($format) );
pod2usage(1) if ( $action eq 'restore_rmt' && !($dir && $host && $id && $location) );
pod2usage(1) if ( $action eq 'dump_rmt' && !($dir && $host && $id && $location && $dbname) );

my $hostBkpDir = $dir . "/" . $host;

my %params = (
    "bkpDir" => $dir,
    "host" => $host,
    "user" => $user,
    "pass" => $pass,
    "location" => $location,
    "dbname" => $dbname,
    "bkpType" => $type,
    "uuid" => $id,
    "hostBkpDir" => $hostBkpDir,
    "format" => $format,
    "socket" => $socket,
    "optimize" => $optimize
);

$dbgLogger->debug("Dumping parameters: ", sub { Dumper(\%params) } );
$dbgLogger->debug("Starting action:", $action);

my $backupObj = Backup::Backup->new();

$params{'socket'} = $socket ? $socket : $backupObj->{'socket'};
    
$backupObj->$action(%params);

__END__

=head1 NAME

        myback

=head1 SYNOPSIS

        myback [--help|-h] [--action|-a] action [--dir|-d] path [--host|-h] host 
                [--type|-b] backup_type [--user|-u] database_user
                [--pass|-s] password [--loc|-l] location_of_mysql_dir
                [--id|-i] id_of_backup [--dbname|-n] database_name
                [--socket|o] socket

=head1 DESCRIPTION

        Utility for backing up mysql, backups whole server, full or incremental
        backups

=head1 OPTIONS

=over 8

=item B<--help|-h>

        prints help for utility

=item B<--action|-a>

    Can be one of these:
    
    backup
        creates local backup to the directory dir
        requires: dir, user, password, backup_type, name of database host
        optional: socket, optimize
        
    restore
        creates local restore to the directory loc
        requires: dir, host, id, location, user, pass
        
    list
        lists backups on host
        requires: host, format, user, pass
        optional: socket
        
    rmt_backup
        initiates backup on remote host, backups remote host and transfer
        backup on server to directory dir, insert info about backup to server database
        requires: dir, host - host is alias for the remote host
        (there can be several db ports on one host...), type, optimize
        
    list_rmt
        lists backups from remote hosts, which where transfered to the server
        during rmt_backup
        requires: format
        
    restore_rmt
        restores backup from remote hosts, stored on server to the location loc
        from dir
        requires: dir, host (alias), id, location
        
    dump_rmt
        dumps backup from remote hosts, stored on server to the location loc
        from dir
        requires: dir, host, id, location, dbname
        optional: socket
        
=item B<--dir|-d>

    This is path to the directory, where backups are present, whether remote
    or local

=item B<--host|-h>

    this is hostname or alias for which you want to make backup, depends
    on the action you are executing, if you are executing remote commands
    it means alias of host because in db records on server one host can have
    multiple aliases, because host can have databases on multiple ports

=item B<--type|-b>

    This is type of backups you want to execute, can be:

    incremental
    full
        
=item B<--user|-u>

    Database user
        
=item B<--pass|-s>

    Database password
        
=item B<--loc|-l>

    this is directory path where restores/dumps are decompressed and restored
        
=item B<--id|-i>

    every backup has uniq uuid
        
=item B<--dbname|-n>

    this is valid option just for dump_rmt action, specifies which databases
    we want to dump, supplied as comma separated list, we can specify: all,
    for dumping all databases in one file
     
=item B<--socket|-o>

    database socket
      
=item B<--optimize|-z>

    this is used to determine if mysql optimize should be run before backup
    NOTE: if you use incremental option value with optimize you will see probably
    same size of incremental backup as full backup, this is because mysql innodb tables
    are recreated and thus considered changed
    
=cut

=back

=head1 EXAMPLES

    We have two servers host1 and host2.
    
    We are on the host with name host1. We are making local backup of mysql
    database. It will be written to the /backups directory. Mysql user is backup
    , password is backup, type of backup is full. Mysql database host is localhost,
    if mysql would be listinening on host1 IP, than there would be host1:
    
    myback -a backup -d /backups -u backup -p backup -b full -h localhost -z yes
    
    same, but incremental backups and we can specify socket if it is different
    than standard:
    
    myback -a backup -d /backups -u backup -p backup -b incremental -h localhost -o /var/run/mysqld/mysqldCUSTOM.sock -z no
    
    command restores local mysql backup with uuid 06ad4560-007e-11e5-973c-005056850000
    and for database host localhost to directory /restore (WARNING: it first
    removes /restore dir, it creates it again and then restores):
    
    myback -a restore -d /backups -h localhost -i 06ad4560-007e-11e5-973c-005056850000 -l /restore
    
    lists local backup in table format, needs local mysql user and password and
    mysql host:
    
    myback -a list -u backup -p backup -f tbl -h localhost
    
    we are on the host2, which acts as server for storing backups, it has database
    of hosts and their configurations which are remotely backuped - clients. One
    client can have several mysql instances running on different ports, thus
    in configurations we use for each port separate alias, e.g. in our case
    we have instance running with custom socket /var/run/mysqld/mysqldCUSTOM.sock,
    so we gave host1_CUSTOM alias to our database. This is doing full remote backup
    of remote mysql instance with alias host1_CUSTOM and stores it on backup
    server host2 in location /remote/backups. On backup server host2 for each
    remote host are created dirs in /remote/backups
    
    myback -a rmt_backup -d /remote/backups -h host1_CUSTOM -b full
    
    on host2 lists all backups of remote backup clients in list format
    
    myback -a list_rmt -f lst
    
    on host2 we are restoring backup of remote backup with uuid
    8b65d676-15c5-11e5-a6ea-0ef14e8692d9 and for backup client host1_CUSTOM
    to directory /remote/restore on backup server host2
    
    myback -a restore_rmt -d /remote/backups -i 8b65d676-15c5-11e5-a6ea-0ef14e8692d9 -h host1_CUSTOM -l /remote/restore
    
    same as above, but additionaly automatically creates dump of database
    customer_db 
    
    myback -a dump_rmt -d /remote/backups -i 8b65d676-15c5-11e5-a6ea-0ef14e8692d9 -h host1_CUSTOM -l /remote/restore -n customer_db
    
    same as previous, but dumps all databases. NOTE: before doing this, mysql
    server should not be running on backups server host2, after restore, mysql
    server will be again shutdown, to be able to do dump again automatically
    (script runs its own mysql instance with configuration from remote backup client):
    
    myback -a dump_rmt -d /remote/backups -i 8b65d676-15c5-11e5-a6ea-0ef14e8692d9 -h host1_CUSTOM -l /remote/restore -n all
    
=head1 AUTHOR

    PAVOL IPOTH <pavol.ipoth@gmail.com>

=head1 COPYRIGHT

    Pavol Ipoth, ALL RIGHTS RESERVED, 2015

=head1 License

    GPLv3

=cut
