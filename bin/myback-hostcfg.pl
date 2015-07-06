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

use Backup::Db::Config;

# configuring our logger, we have two one for debug output
# and one for normal output, cause they have different formats
my $log_conf = "$FindBin::RealBin/../etc/log-cfg.conf";
Log::Log4perl::init($log_conf);

my $dbgLogger = Log::Log4perl->get_logger('debug');
my $baseLogger = Log::Log4perl->get_logger('base');

$dbgLogger->level($DEBUG);
$baseLogger->level($INFO);

$dbgLogger->debug('Starting');
$dbgLogger->debug('Parsing arguments');

my $help = 0;
my $action = '';
my $host = '';
my $ip = '';
my $privKey = '';
my $user = '';
my $pass = '';
my $alias = '';
my $localHost = '';
my $localDir = '';
my $socket = '';

my $allowedActions = {
                        'add' => 1,
                        'delete' => 1,
                        'list' => 1
                    };
                    
pod2usage(-verbose => 3) unless @ARGV;

GetOptions(
                'action|a=s' => \$action,
                'host|h=s' => \$host,
                'ip|i=s' => \$ip,
                'privKey|k=s' => \$privKey,
                'user|u=s' => \$user,
                'pass|p=s' => \$pass,
                'alias|s=s' => \$alias,
                'localHost|l=s' => \$localHost,
                'localDir|d=s' => \$localDir,
                'socket|o=s' => \$socket,
                'help!' => \$help
        ) or pod2usage(-verbose => 3);

pod2usage(-verbose => 3) if $help;
pod2usage(1) if !$action;
pod2usage(-verbose => 3) if !defined( $allowedActions->{$action} );

if( $action eq 'add' && !(
                            $host && $ip && $privKey && 
                            $user && $pass && $alias && 
                            $localHost && $localDir && $socket
                            ) 
) {
    pod2usage(1); 
} # if

pod2usage(1) if ( $action eq 'delete' && !($host && $alias) );

my %params = (
    'host' => $host,
    'ip' => $ip,
    'privKey' => $privKey,
    'user' => $user,
    'pass' => $pass,
    'alias' => $alias,
    'localHost' => $localHost,
    'localDir' => $localDir,
    'socket' => $socket
);

$dbgLogger->debug("Dumping parameters: ", sub { Dumper(\%params) } );
$dbgLogger->debug("Starting action:", $action);

my $bkpConfig = Backup::Db::Config->new();
$bkpConfig->$action(%params);

__END__

=head1 NAME

        myback-hostcfg

=head1 SYNOPSIS

    myback-hostcfg [--help|-h] [--action|-a] action [--host|-h] host 
            [--ip|-i] IP of host [--privKey|-k] path
            [--user|-u] user [--pass|-p] password [--alias|-s] alias
            [--localHost|-l] local host [--localDir|-d] local directory
            [--socket|-o] path
                

=head1 DESCRIPTION

    Utility for adding and removing backup configurations for backup clients,
    primarily aimed for use with ansible
        
=head1 OPTIONS

=over 8

=item B<--help|-h>

    prints help for utility

=item B<--action|-a>

    Can be one of these:
    
    add
        adds entry to the database, if host is present but alias not, alias
        is added to the host, if alias is present doing nothing and exiting
        with zero code (this is to not interrupt ansible)
        requires: host, ip, privKey, user, pass, alias, localHost, localDir, socket
        
    delete
        delete entries for host, if we specify alias as: all, it removes all
        configurations and aliases for host, if we specify alias, it removes
        only specified alias
        requires: host, alias
                
    list
        lists all entries in YAML format, again because of ansible
        
=item B<--host|-h>

    This is path to the directory, where backups are present
        
=item B<--ip|-i>

    this is directory path where backups are downloaded
        
=item B<--privKey|-k>

    path to the file with private key
  
=item B<--user|u>

    mysql user with which we connect on remote host to mysql db
   
=item B<--pass|p>

    mysql password for user
        
=item B<--alias|-s>

    our custom alias for host
    
=item B<--localHost|-l>

    this is mysql host to which we connect when performing backup on remote host
    
=item B<--localDir|-d>

    this is directory on remote host, this is used by backup utilities
    
=item B<--socket|-o>

    socket, path to the mysql socket on remote host
    
=back

=head1 EXAMPLES

    We have host1 with two mysql instances running on ports 3307 and 3308.
    We have empty configuration database.
    
    command adds host1 to the database and also its alias configuration for
    instance running on port 3307:
    
    myback-hostcfg -a add -h host1 -i 192.171.220.165 
    -k /home/backup/.ssh/private_key 
    -u backup -p backup1 -s host1_3307 -l localhost -d /home/backups 
    -o /var/run/mysqld/mysqld3307.sock
    
    as host1 is already in our database but alias for second mysql instance
    running on port 3308, we add second alias configuration to database:
    
    myback-hostcfg -a add -h host1 -i 192.171.220.165 
    -k /home/backup/.ssh/private_key 
    -u backup -p backup2 -s host1_3308 -l localhost -d /home/backups 
    -o /var/run/mysqld/mysqld3308.sock
    
    command deletes configuration for alias host1_3307:
    
    myback-hostcfg -a delete -h host1 -s host1_3307
    
    command deletes all entries for host1 in database:
    
    myback-hostcfg -a delete -h host1 -s all
    
    if we run this command again, it does nothing and returns zero code, this
    is because we want to use it with ansible
    
    myback-hostcfg -a delete -h host1 -s all
    
=head1 AUTHOR

    PAVOL IPOTH <pavol.ipoth@gmail.com>

=head1 COPYRIGHT

    Pavol Ipoth, ALL RIGHTS RESERVED, 2015

=head1 License

    GPLv3

=cut
