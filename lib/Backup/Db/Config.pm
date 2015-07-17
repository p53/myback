package Backup::Db::Config;

=head1 NAME

    Backup::Db::Config - module for maintaing host configurations for backup

=head1 SYNOPSIS

    my $backupCfgObj = Backup::Db::Config->new();
    
    $backupCfgObj->add(     
                            'host' => 'host1',
                            'ip' => '192.168.5.25',
                            'privKey' => '/home/.ssh/backup_priv_key',
                            'user' => 'backup',
                            'pass' => 'simple',
                            'alias' => 'host1',
                            'localHost' => 'localhost',
                            'localDir' => '/tmp',
                            'socket' => '/var/run/mysqld/mysqld.sock'
                        );

=cut

use Moose;
use namespace::autoclean;
use Carp;
use Try::Tiny;
use warnings;
use autodie;
use File::Path;
use File::Find;
use File::Copy;
use POSIX;
use DBI;
use IO::File;
use Data::Dumper;
use File::stat;
use YAML::Tiny;

with 'Backup::Db::DbInterface', 'MooseX::Log::Log4perl';

=head1 METHODS

=over 12

=item C<add>

method adds record to the host configuration database, if host is already
present but alias not it will add it

param:

    host string - name of host
    ip string - IP address of host
    privKey string - path to the private key file
    user string - mysql database user
    pass string - mysql database password
    alias string - alias for host configuration
    localHost string - local name of database host
    localDir string - local directory of added host
    socket string - socket path of database host
    
return:

    void

=cut

sub add {

    my $self = shift;
    my %params = @_;
    my $host = $params{'host'};
    my $ip = $params{'ip'};
    my $privKeyFile = $params{'privKey'};
    my $user = $params{'user'};
    my $pass = $params{'pass'};
    my $alias = $params{'alias'};
    my $localHost = $params{'localHost'};
    my $localDir = $params{'localDir'};
    my $socket = $params{'socket'};
    
    my $dbh = $self->localDbh;
    my $sth;
    
    $self->log('base')->info('Checking if alias is present');
    
    my $aliasCheckQuery = "SELECT COUNT(bkpconf_id) AS numAlias from bkpconf";
    $aliasCheckQuery .= " WHERE alias=?";
    
    $self->log('debug')->debug("Query: ", sub { Dumper($aliasCheckQuery) } );
    
    try {
        $sth = $dbh->prepare($aliasCheckQuery);
        $sth->bind_param(1, $alias);
        $sth->execute();
    } catch {
        my $error = @_ || $_;
        $self->log('base')->error("Error: ", $error, " Query: " . $aliasCheckQuery);
        croak "Error: " . $error;
    }; # try
    
    my @checkResult = ();
    
    while( my $row = $sth->fetchrow_hashref() ) {
        push(@checkResult, $row);
    } # while
    
    $self->log('debug')->debug("Dumping result: ", sub { Dumper(\@checkResult) } );
    
    if( $checkResult[0]->{'numAlias'} > 0 ) {
        $self->log('base')->error("Alias already present, alias: ", $alias);
        print "Alias already present, alias: " . $alias . "\n";
        exit;
    } # if
    
    $self->log('base')->info('Checking if private key file is valid. file: ', $privKeyFile);
    
    if( ! -r $privKeyFile) {
        $self->log('base')->error("Private key file does not exist or is not readable! file: ", $privKeyFile);
        croak "Private key file does not exist or is not readable! file: " . $privKeyFile;
    } # if
    
    my $hostCheckQuery = "SELECT host_id FROM host WHERE host_name=?";
    
    $self->log('debug')->debug("Query: ", sub { Dumper($hostCheckQuery) } );
    
    try {
        $sth = $dbh->prepare($hostCheckQuery);
        $sth->bind_param(1, $host);
        $sth->execute();
    } catch {
        my $error = @_ || $_;
        $self->log('base')->error("Error: ", $error, " Query: " . $hostCheckQuery);
        croak "Error: " . $error;
    }; # try
    
    my @hostCheckResult = ();
    
    while( my $row = $sth->fetchrow_hashref() ) {
        push(@hostCheckResult, $row);
    } # while
    
    $self->log('debug')->debug("Dumping result: ", sub { Dumper(\@hostCheckResult) } );
    
    if( scalar(@hostCheckResult) > 1 ) {
        croak "Found two records with same host name, something is bad with your db! host: " . $host;
    } # if
    
    $self->log('base')->info('Reading private key file. file: ', $privKeyFile);

    my $file = IO::File->new($privKeyFile, 'r');
    my @lines = $file->getlines();
    $file->close();
    
    my $privKeyStr = join('', @lines);
    
    if( scalar(@hostCheckResult) == 1 ) {
    
        my $bkpConfInsert = {
           'host_id' => $hostCheckResult[0]->{'host_id'},
           'priv_key' => $privKeyStr,
           'user' => $user,
           'pass' => $pass,
           'alias' => $alias,
           'local_dir' => $localDir,
           'local_host' => $localHost,
           'socket' => $socket
        };
        
        $self->log('debug')->debug("Dumping inserted data: ", sub { Dumper($bkpConfInsert) } );
        
        $self->log('base')->info('Inserting config for host, host already present. host: ', $host);
        
        try {
            $self->hashInsert('table' => 'bkpconf', 'data' => $bkpConfInsert);
        } catch {
            my $error = @_ || $_;
            $self->log('base')->error("Error: ", $error);
            croak "Error: " . $error;
        }; # try
        
    } else {
        
        my $hostInsert = {
            'host_name' => $host,
            'ip' => $ip
        };

        $self->log('debug')->debug("Dumping data being inserted: ", sub { Dumper($hostInsert) } );
        
        $self->log('base')->info('Inserting basic data for host. host: ', $host);
        
        try {
            $self->hashInsert('table' => 'host', 'data' => $hostInsert);
        } catch {
            my $error = @_ || $_;
            $self->log('base')->error("Error: ", $error);
            croak "Error: " . $error;
        }; # try
        
        my $id = $dbh->last_insert_id("", "", "host", "");
        
        my $bkpConfInsert = {
           'host_id' => $id,
           'priv_key' => $privKeyStr,
           'user' => $user,
           'pass' => $pass,
           'alias' => $alias,
           'local_dir' => $localDir,
           'local_host' => $localHost,
           'socket' => $socket
        };
        
        $self->log('debug')->debug("Dumping data being inserted: ", sub { Dumper($bkpConfInsert) } );
        
        $self->log('base')->info('Inserting config for host. host: ', $host);
        
        try {
            $self->hashInsert('table' => 'bkpconf', 'data' => $bkpConfInsert);
        } catch {
            my $error = @_ || $_;
            $self->log('base')->error("Error: ", $error);
            croak "Error: " . $error;
        }; # try
    
    } # if
    
} # end sub add

=item C<delete>

Method deletes host configuration for host or just alias configuration for host

param:

    host string - name of host
    alias string - alias for host
    
return:

    void
    
=cut

sub delete {

    my $self = shift;
    my %params = @_;
    my $host = $params{'host'};
    my $alias = $params{'alias'};

    my $dbh = $self->localDbh;
    my $sth;
    
    if( $alias eq 'all' ) {
    
        my $delBkpConfQuery = "DELETE from bkpconf WHERE host_id=";
        $delBkpConfQuery .= "(SELECT host_id FROM host WHERE host_name=?)";
        
        $self->log('debug')->debug("Query: ", sub { Dumper($delBkpConfQuery) } );
                
        try {
            $sth = $dbh->prepare($delBkpConfQuery);
            $sth->bind_param(1, $host);
            $sth->execute();
        } catch {
            my $error = @_ || $_;
            $self->log('base')->error("Error: ", $error, " Query: " . $delBkpConfQuery);
            croak "Error: " . $error;
        };
        
        $self->log('base')->info('Deleting all entries for host. host: ', $host);
        
        my $delHostQuery = "DELETE from host WHERE host_name=?";

        $self->log('debug')->debug("Query: ", sub { Dumper($delHostQuery) } );
        
        try {
            $sth = $dbh->prepare($delHostQuery);
            $sth->bind_param(1, $host);
            $sth->execute();
        } catch {
            my $error = @_ || $_;
            $self->log('base')->error("Error: ", $error, " Query: " . $delHostQuery);
            croak "Error: " . $error;
        };
        
    } else {
    
        $self->log('base')->info('Deleting alias for host. host: ' . $host . ', alias: ' . $alias);
        
        my $delBkpConfQuery = "DELETE from bkpconf WHERE alias=?";
        
        $self->log('debug')->debug("Query: ", sub { Dumper($delBkpConfQuery) } );
                
        try {
            $sth = $dbh->prepare($delBkpConfQuery);
            $sth->bind_param(1, $alias);
            $sth->execute();
        } catch {
            my $error = @_ || $_;
            $self->log('base')->error("Error: ", $error, " Query: " . $delBkpConfQuery);
            croak "Error: " . $error;
        };
    
    } # if
    
} # end sub delete

=item C<list>

Method returns list of all alias configurations as YAML

param:

return:

    $yamlStr string - list of all alias configurations
    
=cut

sub list {

    my $self = shift;
    my %params = @_;
    my $dbh = $self->localDbh;
    my $sth;

    my $listQuery = "SELECT host_name, ip, alias, local_host, local_dir, socket, user";
    $listQuery .= " FROM host JOIN bkpconf ON host.host_id=bkpconf.host_id ORDER BY alias";
    
    try {
        $sth = $dbh->prepare($listQuery);
        $sth->execute();
    } catch {
        my $error = @_ || $_;
        $self->log('base')->error("Error: ", $error, " Query: " . $listQuery);
        croak "Error: " . $error;
    };   
    
    my @entries = ();
    
    while( my $row = $sth->fetchrow_hashref() ) {
        push(@entries, $row);
    } # while

    my $yamlStr = YAML::Tiny::Dump(@entries);
    
    print $yamlStr . "\n";
    
} # end sub list

no Moose::Role;

=head1 AUTHOR

    PAVOL IPOTH <pavol.ipoth@gmail.com>

=head1 COPYRIGHT

    Pavol Ipoth, ALL RIGHTS RESERVED, 2015

=head1 License

    GPLv3

=cut

1;
