#!/usr/bin/perl -w

BEGIN {
        use Cwd 'abs_path';
        # getting absolute path to this script and get current folder and library path
        my $scriptPath = abs_path($0);
        $scriptPath =~ s/^(.*)(\/|\\).*$/$1/;
        our $classesPath = $scriptPath . '/lib';

} # BEGIN

use lib $main::classesPath;

use strict;
use warnings;
use autodie;
use Getopt::Long;
use Pod::Usage;
use Sys::Hostname;
use DateTime;

use Backup::Backup;

my $help = 0;
my $action = '';
my $dir = '';
my $host = '';
my $user = '';
my $port = 0;
my $pass = '';
my $date = '';
my $location = '';
my $dbname = '';
my $config = '';
my $type = '';
my $id = '';
my $format = '';

my $allowedTypes = {
                        'incremental' => 1,
                        'full' => 1
                    };

my $allowedActions = {
                        'backup' => 1,
                        'restore' => 1,
                        'dump' => 1,
                        'list' => 1
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
                'port|p:i' => \$port,
                'pass|s:s' => \$pass,
                'date|t:s' => \$date,
                'loc|l:s' => \$location,
                'id|i:s' => \$id,
                'dbname|n:s' => \$dbname,
                'config|c:s' => \$config,
                'format|f:s' => \$format,
                'help!' => \$help
        ) or pod2usage(-verbose => 3);

pod2usage(-verbose => 3) if $help;
pod2usage(1) if !$action;
pod2usage(1) if !$dir;
pod2usage(1) if !$host;

pod2usage(-verbose => 3) if !defined( $allowedActions->{$action} );
pod2usage(1) if ( $action eq 'restore' && !($dir && $host && $id && $location) );
pod2usage(1) if ( $action eq 'list' && !($dir && $host && $format) );
pod2usage(1) if ( $action eq 'list' && !defined( $allowedFormats->{$format} ) );

pod2usage(1) if ( $action eq 'backup' && !defined( $allowedTypes->{$type} ) );

if( $action eq 'backup' && !($dir && $type && $host && $user && $port && $pass) ) {
    pod2usage(1);
}

if( $action eq 'dump' && !($dir && $date && $location && $dbname && $host) ) {
    pod2usage(1);
}

my $hostBkpDir = $dir . "/" . $host;

my %params = (
    "bkpDir" => $dir,
    "host" => $host,
    "user" => $user,
    "port" => $port,
    "pass" => $pass,
    "date" => DateTime->now,
    "location" => $location,
    "dbname" => $dbname,
    "bkpType" => $type,
    "uuid" => $id,
    "hostBkpDir" => $hostBkpDir,
    "format" => $format
);

my $backupObj = Backup::Backup->new(
                                    'bkpDir' => $dir, 
                                    'host' => $host,
                                    'hostBkpDir' => $hostBkpDir
                                );

$backupObj->$action(%params);


__END__

=head1 NAME

        myback

=head1 SYNOPSIS

        myback [--help] [--action|-a] action [--dir|-d] path [--host|-h] database_host 
                [--type|-t] backup_type [--user|-u] database_user [--port|-p] database_port
                [--pass|-s] password [--date|-t] date_of_backup [--loc|-l] location_of_mysql_dir
                [--id|-i] id_of_backup [--dbname|-n] database_name [--config|-c] config_path

=head1 DESCRIPTION

        Utility for backing up mysql

=head1 OPTIONS

=over 8

=item B<--help>

        prints help for utility

=item B<--file>

        location of the SRTS html file

=item B<--type>

        name of supported storage type to convert to ASM format
        Currently supported types: EMC, HITACHI

=item B<--host>

        this is hostname for which you want to produce output

=back

=head1 AUTHOR

        PAVOL IPOTH <pavol.ipoth@gmail.com>

=head1 COPYRIGHT

        Pavol Ipoth, ALL RIGHTS RESERVED, 2014

=head1 License

        GPLv3

=cut
