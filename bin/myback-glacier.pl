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
use lib "$FindBin::RealBin/../lib";

use Backup::Glacier;

my $glcConfigPath = "$FindBin::RealBin/../etc/glacier.cfg";

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
my $location = '';
my $id = '';
my $time = '';
my $format = '';

my $allowedActions = {
                        'sync' => 1,
                        'clean' => 1,
                        'clean_rmt' => 1,
                        'clean_journal' => 1,
                        'get' => 1,
                        'list' => 1,
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
pod2usage(-verbose => 3) if !defined( $allowedActions->{$action} );

pod2usage(1) if ( $action eq 'sync' && !($dir) );
pod2usage(1) if ( $action eq 'clean' && !($dir && $time) );
pod2usage(1) if ( $action eq 'clean_rmt' && !($dir && $time) );
pod2usage(1) if ( $action eq 'clean_journal' && !($dir && $time) );
pod2usage(1) if ( $action eq 'get' && !($dir && $id && $location) );
pod2usage(1) if ( $action eq 'list' && !($dir && $format) );
pod2usage(1) if ( $action eq 'list' && !defined( $allowedFormats->{$format} ) );

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

        myback-glacier [--help|-h] [--action|-a] action [--dir|-d] path 
                [--loc|-l] location_of_mysql_dir [--time|-t] time
                [--id|-i] id_of_backup [--format|f] output format

=head1 DESCRIPTION

        Utility for uploading and managing backups between host and glacier
        
=head1 OPTIONS

=over 8

=item B<--help|-h>

        prints help for utility

=item B<--action|-a>

    Can be one of these:
    
    sync
        uploads all files, which are not yet in glacier, in dir recursively to the
        glacier
        requires: dir
        
    get
        retrieves and downloads files with uuid as supplied id to the directory
        specified in location parameter
        requires: dir, id, location
                
    clean
        select all entries which have backup start older than supplied time,
        checks if entries present on filesystem, if yes check if present 
        in glacier table, if yes delete from database and also from filesystem, 
        if not present on fs nor in glacier delete just entry in database
        requires: dir, time
        
    clean_rmt
        select all entries from glacier table which have backup start older
        than supplied time, delete them from glacier table
        requires: dir, time
        
    clean_journal
        selects all archive entries from journal table, which have delete entries
        older than supplied time and are not present in glacier table
        requires: dir, time
        
    list
        lists backups, which should be present in glacier
        requires: dir, format
        
=item B<--dir|-d>

    This is path to the directory, where backups are present
        
=item B<--loc|-l>

    this is directory path where backups are downloaded
        
=item B<--id|-i>

    every backup has uniq uuid
  
=item B<--time|t>

    number of days/hours/minutes in one of format: 1d or 24h or 1440m
   
=item B<--format|f>

    output format, currently supported

    tbl - table
    lst - list
        
=back

=head1 EXAMPLES

    We have host myhost and backups in /backups folder. Every action with glacier
    is logged to journal.
    
    Command uploads all content of directory /backups to the glacier. It is uploading
    just in case files are not present already in glacier and their mtime is not
    different:
    
    myback -a sync -d /backups
    
    command retrievs, waits for retrieve end (in glacier it takes approx. 4+ hours)
    and downloads backup with uuid ffaafb60-158e-11e5-a6ea-0ef14e8692d9
    to directory /glacier/download:
    
    myback -a get -d /backups -l /glacier/download -i ffaafb60-158e-11e5-a6ea-0ef14e8692d9
    
    command cleans local history of remote backups and remove those backup from
    filesystem, for entries older than 7200 hours, check clean method for details
    (it is not so simple):
    
    myback -a clean -d /backups -t 7200h
    
    command cleans local history of glacier backups and remove those backups
    from glacier, for entries older than 30 days, please check cleam_rmt for
    details:
    
    myback -a clean_rmt -d /backups -t 30d
    
    command cleans journal entries older than 90 days, again it is not so simple,
    please check clean_journal method description:
    
    myback -a clean_journal -d /backups -t 90d
    
    command lists backups which should be present now in glacier:
    
    myback -a list -d /backups -f tbl
    
=head1 AUTHOR

    PAVOL IPOTH <pavol.ipoth@gmail.com>

=head1 COPYRIGHT

    Pavol Ipoth, ALL RIGHTS RESERVED, 2015

=head1 License

    GPLv3

=cut
