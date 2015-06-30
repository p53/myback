package Backup::BackupInterface;

=head1 NAME

    Backup::BackupInterface - interface for backup object

=head1 SYNOPSIS

    use in classes: with 'Backup::BackupInterface'

=cut

use Moose::Role;
use MooseX::ClassAttribute;
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

=head1 PUBLIC PROPERTIES

=over 12

=item bkpType Backup::Type::*

    stores backup type object generated during run

=cut

has 'bkpType' => (
    is => 'rw'
);

=item socket string
    
    stores default path to mysql socket
    
=cut

has 'socket' => (
    is => 'rw',
    isa => 'Str',
    default => '/var/run/mysqld/mysqld.sock'
);

=item bkpDb string

    stores default path to sqlite database, used for storing
    remote backups information
    
=cut

has 'bkpDb' => (
    is => 'rw',
    default => '/var/lib/myback/bkpdb'
);

=item compression string

    stores current selected type of compression
    
=cut

has 'compression' => (
    is => 'rw',
    isa => 'Str',
    default => 'pigz'
);

=item compressions hash_ref

    hash ref of compression types and their suffixes

=back

=cut

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

=head1 PUBLIC STATIC PROPERTIES

=over 12

=item DBI::db

    stores database handler object

=back

=cut

class_has 'localDbh' =>
        ( is      => 'rw',
          isa     => 'DBI::db',
          default => sub { {} },
        );
        
=head1 METHODS

=over 12

=item C<backup>

    Method not implemented

=cut

sub backup {}

=item C<restore>

    Method not implemented

=cut

sub restore {}

=item C<dump_rmt>

    Method not implemented
    
=cut

sub dump_rmt {}

=item C<restore_rmt>

    Method not implemented
    
=cut

sub restore_rmt {}

=item C<list_rmt>

    Method not implemented
    
=cut

sub list_rmt {}

sub BUILD {

    my $class = shift;
    
    my $dbh = DBI->connect(
                                "dbi:SQLite:dbname=" . $class->{'bkpDb'},
                                "", 
                                "",
                                {'RaiseError' => 1}
                            );
                            
    $class->localDbh($dbh);
    
} # end sub BUILD

=item C<getLastBkpInfo>

Method gets last backup info according start time from local mysql db

param:

    user string - mysql user for local history database
    
    pass string - mysql password for mysql user
    
    socket string - path to mysql socket
    
return:

=cut

sub getLastBkpInfo {

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

=item C<bkpInfoTimeToUTC>

Method converts time in backup info hash_ref to UTC

param:

    bkpInfo hash_ref - information about backup

return:

    bkpInfo hash_ref - information about backup with converted times
    
=cut

sub bkpInfoTimeToUTC {

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

=item C<rmt_tmp_backup>

Method creates remote backup, stores it on remote host and then copies to
server from which remote backup was done

param:

    host string - remote host to backup, this is alias
    
    bkpDir string - directory where backup will be stored on server
    
    bkpType string - this is type of backup we want to execute on remote host
    
return:

    void
    
=cut

sub rmt_tmp_backup {

    my $self = shift;
    my %params = @_;
    my $privKeyPath = '/tmp/' . $params{'host'} . '.priv';
    my $hostInfo = {};
    my @hostsInfo = ();

    $self->log('base')->info("Starting remote backup for host alias ", $params{'host'});

    my $query = "SELECT * FROM host JOIN bkpconf";
    $query .= " ON host.host_id=bkpconf.host_id";
    $query .= " WHERE bkpconf.alias='" . $params{'host'} . "'";

    @hostsInfo = @{ $self->localDbh->selectall_arrayref($query, { Slice => {} }) };

    if( scalar(@hostsInfo) == 0 ) {
        $self->log->error("No such host!");
        croak "No such host!";
    } elsif( scalar(@hostsInfo) > 1 ) {
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
        my @escVals = map { my $s = $_; $s = $self->localDbh->quote($s); $s } @values;

        $self->log('debug')->debug("Dumping imported info: ", sub { Dumper($config) });

        my $query = "INSERT INTO history(" . join( ",", keys(%$config) ) . ",";
        $query .=  "bkpconf_id)";
        $query .= " VALUES(" . join( ",", @escVals ). "," . $hostInfo->{'bkpconf_id'} . ")";
        
        my $sth = $self->localDbh->prepare($query);
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

    $self->log('base')->info("Removing temporary private key file");

    unlink $privKeyPath;

    $self->log('base')->info("Backup successful");

} # end sub rmt_tmp_backup

=item C<mysqlXmlToHash>

Converts mysql xml response to simple hash

param:

    xml string - xml response from mysql server
    
return:

    data hash_ref
    
=back

=cut

sub mysqlXmlToHash {

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

=head1 AUTHOR

        PAVOL IPOTH <pavol.ipoth@gmail.com>

=head1 COPYRIGHT

        Pavol Ipoth, ALL RIGHTS RESERVED, 2015

=head1 License

        GPLv3

=cut

1;
