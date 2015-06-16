package Backup::BackupInterface;

use Moose::Role;
use namespace::autoclean;
use Moose::Util::TypeConstraints;
use Carp;
use Try::Tiny;
use warnings;
use autodie;
use File::Glob;
use File::Copy;
use File::Path;
use File::Basename;
use IO::File;
use DateTime;
use Data::Dumper;
use DBI;
use XML::LibXML;

subtype 'DirectoryExists',
    => as 'Str'
    => where { -d $_ }
    => message { "Directory $_ does not exist!" };

has 'bkpDir' => (
    is => 'rw',
    isa => 'DirectoryExists',
    required => 1
);

has 'bkpType' => (
    is => 'rw'
);

has 'hostBkpDir' => (
    is => 'rw',
    isa => 'Str'
);

has 'host' => (
    is => 'rw',
    isa => 'Str'
);

has 'user' => (
    is => 'rw',
    isa => 'Str'
);

has 'pass' => (
    is => 'rw',
    isa => 'Str'
);

has 'location' => (
    is => 'rw',
    isa => 'Str'
);

has 'dbname' => (
    is => 'rw',
    isa => 'Str'
);

has 'socket' => (
    is => 'rw',
    isa => 'Str',
    default => '/var/run/mysql/mysqld.sock'
);

has 'bkpDb' => (
    is => 'rw',
    default => '/var/lib/myback/bkpdb'
);

has 'compression' => (
    is => 'rw',
    isa => 'Str',
    default => 'pigz'
);

has 'compressions' => (
    is => 'rw',
    isa => 'HashRef',
    default => sub { 
                    return { 
                        'gzip' => 'gz',
                        'bzip2' => 'bz2',
                        'pigz' => 'gz'
                    } 
                }
);

sub backup() {}
sub restore() {}
sub dump_rmt() {}
sub restore_rmt() {}
sub list_rmt() {}

sub getLastBkpInfo() {

    my $self = shift;
    my %params = @_;
    my $info = {};

    my $dbh = DBI->connect(
                            "DBI:mysql:database=PERCONA_SCHEMA;host=localhost;mysql_socket=" . $params{'socket'},
                            $params{'user'}, 
                            $params{'pass'},
                            {'RaiseError' => 1}
                        );

    my $query = "SELECT * FROM PERCONA_SCHEMA.xtrabackup_history";
    $query .= " ORDER BY innodb_to_lsn DESC, start_time DESC LIMIT 1";

    my $sth = $dbh->prepare($query);
    $sth->execute();

    while (my $ref = $sth->fetchrow_hashref) {
        $info = $ref;
    } # while

    $sth->finish;
    $dbh->disconnect();

    return $info; 

} # end sub getLastBackup

sub bkpInfoTimeToUTC() {

    my $self = shift;
    my %params = @_;
    my $bkpInfo = $params{'bkpInfo'};
    
    $bkpInfo->{'start_time'} =~ /(\d{4})-(\d{2})-(\d{2})\s(\d{2})\:(\d{2})\:(\d{2})/;

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

    $bkpInfo->{'start_time'} = $startDt->ymd('-') . ' ' . $startDt->hms(':');

    $bkpInfo->{'end_time'} =~ /(\d{4})-(\d{2})-(\d{2})\s(\d{2})\:(\d{2})\:(\d{2})/;

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

    $bkpInfo->{'end_time'} = $endDt->ymd('-') . ' ' . $endDt->hms(':');
    
    return $bkpInfo;
    
} # end sub bkpInfoTimeToUTC

sub rmt_tmp_backup() {

    my $self = shift;
    my %params = @_;
    my $privKeyPath = '/tmp/' . $params{'host'} . '.priv';
    my $hostInfo = {};
    my @hostsInfo = ();

    $self->log('base')->info("Starting remote backup for host alias ", $params{'host'});

    my $dbh = DBI->connect(
                            "dbi:SQLite:dbname=" . $self->{'bkpDb'},
                            "", 
                            "",
                            {'RaiseError' => 1}
                        );

    my $query = "SELECT * FROM host JOIN bkpconf";
    $query .= " ON host.host_id=bkpconf.host_id";
    $query .= " WHERE bkpconf.alias='" . $params{'host'} . "'";

    @hostsInfo = @{ $dbh->selectall_arrayref($query, { Slice => {} }) };

    if( length(@hostsInfo) == 0 ) {
        $self->log->error("No such host!");
        croak "No such host!";
    } elsif( length(@hostsInfo) > 1 ) {
        $self->log->error("Found more than one alias with that name, check your DB!");
        croak "Found more than one alias with that name, check your DB!";
    } # if

    $hostInfo = $hostsInfo[0];

    if( !( defined $hostInfo->{'user'} && defined $hostInfo->{'pass'} ) ) {
        $self->log->error("You need to specify user, pass for remote host!");
        croak "You need to specify user, pass for remote host!";
    } # if

    my $aliasBkpDir = $params{'bkpDir'} . '/' . $hostInfo->{'alias'};

    $self->log('base')->info("Creating directory on server for alias $aliasBkpDir");

    mkpath($aliasBkpDir) if ! -d $aliasBkpDir;

    my $fh = IO::File->new($privKeyPath, 'w');
    
    print $fh $hostInfo->{'priv_key'};

    $fh->close();

    chmod 0600, $privKeyPath;

    $self->log('base')->info("Executing backup of type $params{'bkpType'} on remote host $hostInfo->{'ip'} on socket $hostInfo->{'socket'}");

    my $rmtBkpCmd = "ssh -i " . $privKeyPath . " " . $hostInfo->{'ip'} . " '";
    $rmtBkpCmd .= 'myback -a backup -b ' . $params{'bkpType'} . ' -u ' . $hostInfo->{'user'};
    $rmtBkpCmd .= " -s \Q$hostInfo->{'pass'}\E -d " . $hostInfo->{'local_dir'};
    $rmtBkpCmd .= ' -o ' . $hostInfo->{'socket'} . ' -h ' . $hostInfo->{'local_host'};
    $rmtBkpCmd .= "'";

    my $shell = Term::Shell->new();
    my $result = $shell->execCmd('cmd' => $rmtBkpCmd, 'cmdsNeeded' => [ 'ssh' ]);

    $shell->fatal($result);

    my $remoteDir = $hostInfo->{'local_dir'} . '/' . $hostInfo->{'local_host'};

    $self->log('base')->info("Syncing remote directory $remoteDir with server directory $aliasBkpDir");

    my $rmtCpCmd = "rsync -avz -e 'ssh -i $privKeyPath' " . $hostInfo->{'ip'} . ":" . $remoteDir . '/*';
    $rmtCpCmd .= " " . $aliasBkpDir;

    $result = $shell->execCmd('cmd' => $rmtCpCmd, 'cmdsNeeded' => [ 'rsync', 'ssh' ]);

    $shell->fatal($result);

    my @bkpConfFiles = glob($aliasBkpDir . '/*/*.yaml');

    $self->log('base')->info("Starting import info about copied backups from yaml files");

    for my $bkpConf(@bkpConfFiles) {
        
        $self->log('debug')->debug("Reading $bkpConf");

        my $yaml = YAML::Tiny->read($bkpConf);
        my $config = $yaml->[0];
        
        my @values = values(%$config);
        my @escVals = map { my $s = $_; $s = $dbh->quote($s); $s } @values;

        $self->log('debug')->debug("Dumping imported info: ", sub { Dumper($config) });

        my $query = "INSERT INTO history(" . join( ",", keys(%$config) ) . ",";
        $query .=  "bkpconf_id)";
        $query .= " VALUES(" . join( ",", @escVals ). "," . $hostInfo->{'bkpconf_id'} . ")";
        
        my $sth = $dbh->prepare($query);
        $sth->execute();

        $self->log('debug')->debug("Removing $bkpConf");

        unlink $bkpConf;

        $bkpConf =~ /(.*)\/(.*)\/(.*)\.yaml$/;
        my $dateDir = $2;

        if( !defined $remoteDir || !defined $dateDir) {
            $self->log->error("You don't define remote host directory and date directory!");
            croak "You don't define remote host directory and date directory!";
        } # if

        my $rmtSrcDir = $remoteDir . '/' . $dateDir;
        my $cleanupSrcDir = "ssh -i " . $privKeyPath . " " . $hostInfo->{'ip'} . " '";
        $cleanupSrcDir .= 'rm -rf ' . $rmtSrcDir;
        $cleanupSrcDir .= "'";

        $self->log('debug')->debug("Removing remote directory $rmtSrcDir");

        $result = $shell->execCmd('cmd' => $cleanupSrcDir, 'cmdsNeeded' => [ 'ssh', 'rm' ]);
        
        $shell->fatal($result);

    } # for

    $dbh->disconnect();

    $self->log('base')->info("Removing temporary private key file");

    unlink $privKeyPath;

    $self->log('base')->info("Backup successful");

} # end sub rmt_tmp_backup

sub mysqlXmlToHash() {

    my $self = shift;
    my %params = @_;
    my $xml = $params{'xml'};
    my $data = {};
    
    my $parser = XML::LibXML->new;
    my $doc = $parser->parse_string($xml);
    my $root = $doc->documentElement();
    my @nodeList = $doc->getElementsByTagName('field');
    
    for my $node(@nodeList) {
        my $key = $node->getAttribute('name');
        my $val = $node->textContent;
        $data->{$key} = $val;
    } # for
    
    return $data;
    
} # end sub mysqlXmlToHash

no Moose::Role;

1;
