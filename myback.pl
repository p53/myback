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
use lib "$FindBin::Bin/lib";

use Backup::Backup;

# configuring our logger, we have two one for debug output
# and one for normal output, cause they have different formats
my $log_conf = "$FindBin::Bin/etc/log.conf";
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
                'help!' => \$help
        ) or pod2usage(-verbose => 3);

pod2usage(-verbose => 3) if $help;
pod2usage(1) if !$action;

pod2usage(-verbose => 3) if !defined( $allowedActions->{$action} );

pod2usage(1) if ( $action eq 'restore' && !($dir && $host && $id && $location) );
pod2usage(1) if ( $action eq 'list' && !($host && $format && $user && $pass) );
pod2usage(1) if ( $action eq 'list' && !defined( $allowedFormats->{$format} ) );

pod2usage(1) if ( $action eq 'backup' && !defined( $allowedTypes->{$type} ) );

if( $action eq 'backup' && !($dir && $type && $host && $user && $pass) ) {
    pod2usage(1);
} # if

if( $action eq 'dump' && !($dir && $location && $dbname && $host) ) {
    pod2usage(1);
} # if

pod2usage(1) if ( $action eq 'rmt_backup' && !($dir && $host && $type) );
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
    "socket" => $socket
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

        Utility for backing up mysql

=head1 OPTIONS

=over 8

=item B<--help|-h>

        prints help for utility

=item B<--action|-a>

    Can be one of these:
    
    backup
        creates local backup to the directory dir
        requires: dir, user, password, backup_type, name of database host
        optional: socket
        
    restore
        creates local restore to the directory loc
        requires: dir, host, id, location
        
    list
        lists backups on host
        requires: host, format, user, pass
        optional: socket
        
    rmt_backup
        initiates backup on remote host, backups remote host and transfer
        backup on server to directory dir, insert info about backup to server database
        requires: dir, host - host is alias for the remote host
        (there can be several db ports on one host...), type
        
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
        
=back

=head1 AUTHOR

        PAVOL IPOTH <pavol.ipoth@gmail.com>

=head1 COPYRIGHT

        Pavol Ipoth, ALL RIGHTS RESERVED, 2015

=head1 License

        GPLv3

=cut
