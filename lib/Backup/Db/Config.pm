package Backup::Db::Config;

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
        $self->log->error("Error: ", $error, " Query: " . $aliasCheckQuery);
        croak "Error: " . $error;
    };
    
    my @checkResult = ();
    
    while( my $row = $sth->fetchrow_hashref() ) {
        push(@checkResult, $row);
    } # while
    
    $self->log('debug')->debug("Dumping result: ", sub { Dumper(\@checkResult) } );
    
    if( $checkResult[0]->{'numAlias'} > 0 ) {
        $self->log->error("Alias already present, alias: ", $alias);
        print "Alias already present, alias: " . $alias . "\n";
        exit;
    } # if
    
    $self->log('base')->info('Checking if private key file is valid. file: ', $privKeyFile);
    
    if( ! -r $privKeyFile) {
        $self->log->error("Private key file does not exist or is not readable! file: ", $privKeyFile);
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
        $self->log->error("Error: ", $error, " Query: " . $hostCheckQuery);
        croak "Error: " . $error;
    };
    
    my @hostCheckResult = ();
    
    while( my $row = $sth->fetchrow_hashref() ) {
        push(@hostCheckResult, $row);
    }
    
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
            $self->log->error("Error: ", $error);
            croak "Error: " . $error;
        };
        
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
            $self->log->error("Error: ", $error);
            croak "Error: " . $error;
        };
        
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
            $self->log->error("Error: ", $error);
            croak "Error: " . $error;
        };
    
    } # if
    
} # end sub add

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
            $self->log->error("Error: ", $error, " Query: " . $delBkpConfQuery);
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
            $self->log->error("Error: ", $error, " Query: " . $delHostQuery);
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
            $self->log->error("Error: ", $error, " Query: " . $delBkpConfQuery);
            croak "Error: " . $error;
        };
    
    } # if
    
} # end sub delete

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
        $self->log->error("Error: ", $error, " Query: " . $listQuery);
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

1;
