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
use File::Copy::Recursive;
use DateTime;
use DBI;
use YAML::Tiny;

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

has 'port' => (
    is => 'rw',
    isa => 'Int'
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
    default => 'data/bkpdb'
);

sub backup() {}
sub restore() {}
sub dump() {}
sub restore_rmt() {}
sub list_rmt() {}

sub getLastBkpInfo() {

    my $self = shift;
    my %params = @_;
    my $info = {};

    my $dbh = DBI->connect(
                            "DBI:mysql:database=PERCONA_SCHEMA;host=localhost;mysql_socket=" . $self->{'socket'},
                            $self->{'user'}, 
                            $self->{'pass'},
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

sub rmt_backup() {

    my $self = shift;
    my %params = @_;
    my $privKeyPath = '/tmp/' . $self->{'host'} . '.priv';
    my $hostInfo = {};
    my @hostsInfo = ();

    my $dbh = DBI->connect(
                            "dbi:SQLite:dbname=" . $self->{'bkpDb'},
                            "", 
                            "",
                            {'RaiseError' => 1}
                        );

    my $query = "SELECT * FROM host JOIN bkpconf";
    $query .= " ON host.host_id=bkpconf.host_id";
    $query .= " WHERE bkpconf.alias='" . $self->{'host'} . "'";

    @hostsInfo = @{ $dbh->selectall_arrayref($query, { Slice => {} }) };

    if( length(@hostsInfo) == 0 ) {
        croak "No such host!";
    } elsif( length(@hostsInfo) > 1 ) {
        croak "Found more than one host with that name, check your DB!";
    } # if

    $hostInfo = $hostsInfo[0];

    if( !( defined $hostInfo->{'user'} && defined $hostInfo->{'pass'} ) ) {
        croak "You need to specify user, pass for remote host!";
    } # if

    my $aliasBkpDir = $self->{'bkpDir'} . '/' . $hostInfo->{'alias'};

    mkpath($aliasBkpDir) if ! -d $aliasBkpDir;

    my $fh = IO::File->new($privKeyPath, 'w');
    
    print $fh $hostInfo->{'priv_key'};

    $fh->close();

    chmod 0600, $privKeyPath;

    my $rmtBkpCmd = "ssh -i " . $privKeyPath . " " . $hostInfo->{'ip'} . " '";
    $rmtBkpCmd .= 'myback -a backup -b ' . $params{'bkpType'} . ' -u ' . $hostInfo->{'user'};
    $rmtBkpCmd .= ' -s ' . $hostInfo->{'pass'} . ' -d ' . $hostInfo->{'local_dir'};
    $rmtBkpCmd .= ' -o ' . $hostInfo->{'socket'} . ' -h ' . $hostInfo->{'local_host'};
    $rmtBkpCmd .= "'";

    my $shell = Term::Shell->new();
    my $result = $shell->execCmd('cmd' => $rmtBkpCmd, 'cmdsNeeded' => [ 'ssh' ]);

    $shell->fatal($result);

    my $remoteDir = $hostInfo->{'local_dir'} . '/' . $hostInfo->{'local_host'};

    my $rmtCpCmd = "rsync -avz -e ssh " . $hostInfo->{'ip'} . ":" . $remoteDir . '/*';
    $rmtCpCmd .= " " . $aliasBkpDir;

    $result = $shell->execCmd('cmd' => $rmtCpCmd, 'cmdsNeeded' => [ 'rsync', 'ssh' ]);

    $shell->fatal($result);

    my @bkpConfFiles = glob($aliasBkpDir . '/*/*.yaml');

    for my $bkpConf(@bkpConfFiles) {
        
        my $yaml = YAML::Tiny->read($bkpConf);
        my $config = $yaml->[0];
        
        my @values = values(%$config);
        my @escVals = map { my $s = $_; $s = $dbh->quote($s); $s } @values;

        my $query = "INSERT INTO history(" . join( ",", keys(%$config) ) . ",";
        $query .=  "bkpconf_id)";
        $query .= " VALUES(" . join( ",", @escVals ). "," . $hostInfo->{'bkpconf_id'} . ")";
        
        my $sth = $dbh->prepare($query);
        $sth->execute();

        unlink $bkpConf;

        $bkpConf =~ /(.*)\/(.*)\/(.*)\.yaml$/;
        my $dateDir = $2;

        if( !defined $remoteDir || !defined $dateDir) {
            croak "You don't define remote host directory and date directory!";
        } # if

        my $rmtSrcDir = $remoteDir . '/' . $dateDir;
        my $cleanupSrcDir = "ssh -i " . $privKeyPath . " " . $hostInfo->{'ip'} . " '";
        $cleanupSrcDir .= 'rm -rf ' . $rmtSrcDir;
        $cleanupSrcDir .= "'";

        $result = $shell->execCmd('cmd' => $cleanupSrcDir, 'cmdsNeeded' => [ 'ssh', 'rm' ]);
        
        $shell->fatal($result);

    } # for

    $dbh->disconnect();

    unlink $privKeyPath;

} # end sub rmt_backup

no Moose::Role;

1;
