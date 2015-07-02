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
use App::MtAws::ConfigEngine;
use lib "$FindBin::Bin/lib";

use Backup::Glacier;

my $glcConfigPath = "$FindBin::Bin/etc/glacier.cfg";

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
my $location = '';
my $id = '';
my $time = '';
my $format = '';

my $allowedActions = {
                        'sync' => 1,
                        'clean' => 1,
                        'clean_rmt' => 1,
                        'clean_journal' => 1,
                        'restore' => 1
                    };

my $allowedFormats = {
                        'tbl' => 1,
                        'lst' => 1
                    };
                    
pod2usage(-verbose => 3) unless @ARGV;

GetOptions(
                'action|a=s' => \$action,
                'dir|d=s' => \$dir,
                'loc|l:s' => \$location,
                'id|i:s' => \$id,
                'time|t:s' => \$time,
                'format|f:s' => \$format,
                'help!' => \$help
        ) or pod2usage(-verbose => 3);

pod2usage(-verbose => 3) if $help;
pod2usage(1) if !$action;

my $configEngine = App::MtAws::ConfigEngine->new();
my $config = $configEngine->read_config($glcConfigPath);

my %params = (
    "bkpDir" => $dir,
    "location" => $location,
    "uuid" => $id,
    "format" => $format,
    "time" => $time,
    "config" => $config
);

$dbgLogger->debug("Dumping parameters: ", sub { Dumper(\%params) } );
$dbgLogger->debug("Starting action:", $action);

my $glacier = Backup::Glacier->new();
$glacier->$action(%params);

__END__

=head1 NAME

        myback-glacier

=head1 SYNOPSIS

        myback [--help|-h] [--action|-a] action [--dir|-d] path [--host|-h] host 
                [--type|-b] backup_type [--user|-u] database_user
                [--pass|-s] password [--loc|-l] location_of_mysql_dir
                [--id|-i] id_of_backup [--dbname|-n] database_name
                [--socket|o] socket

=head1 DESCRIPTION



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
                
=item B<--dir|-d>

        This is path to the directory, where backups are present, whether remote
        or local
        
=item B<--loc|-l>
        this is directory path where restores/dumps are decompressed and restored
        
=item B<--id|-i>
        every backup has uniq uuid
        
=back

=head1 AUTHOR

        PAVOL IPOTH <pavol.ipoth@gmail.com>

=head1 COPYRIGHT

        Pavol Ipoth, ALL RIGHTS RESERVED, 2015

=head1 License

        GPLv3

=cut
