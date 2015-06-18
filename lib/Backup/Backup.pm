package Backup::Backup;

=head1 NAME

    Backup::Backup - module for executing backups

=head1 SYNOPSIS

    my $backupObj = Backup::Backup->new();
    
    $backupObj->backup(     
                            'bkpType' => 'full'
                            'user' => 'backuper',
                            'host' => 'host1',
                            'bkpDir' => '/backups',
                            'hostBkpDir' => '/backups/host1'
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
use Text::SimpleTable;
use DBI;
use Data::Dumper;

use Term::Shell;

with 'Backup::BackupInterface', 'MooseX::Log::Log4perl';

=head1 METHODS

=over 12

=item C<backup>

Method is proxy method, backup gets proper backup type object 
and executes backup method on that object

param:

    bkpType string - requried parameter, backup type to execute
    
    %params - all remaining params, these are passed to produced objects

return:

    void

=cut

sub backup() {

    my $self = shift;
    my %params = @_;

    $self->log('base')->info("Starting local backup");

    if( !( defined $params{'bkpType'} ) ) {
        $self->log->error("You need to specify type!");
        croak "You need to specify type!";
    } # if

    $self->{'bkpType'} = $self->getType(%params);

    $self->{'bkpType'}->backup(%params);
    
} # end sub backup

=item C<rmt_backup>

Method for executing backups on remote hosts and saving them on host from which
we execute this method

param:

    user string - required parameter, database user
    
    host string - required parameter, host where execute remote backup

    pass string - required parameter, database password
    
    bkpDir string - required parameter, directory where backup will be stored
                    on server
    
return:

    void
    
=cut

sub rmt_backup() {

    my $self = shift;
    my %params = @_;
    my $privKeyPath = '/tmp/' . $params{'host'} . '.priv';
    my $hostInfo = {};
    my @hostsInfo = ();
    my $lastBkpInfo = {};
    my $compSuffix = $self->{'compressions'}->{$self->{'compression'}};
    my $compUtil = $self->{'compression'};
    
    $self->log('base')->info("Starting remote backup");

    if( !( defined $params{'bkpType'} && defined $params{'user'} && defined $params{'pass'} ) ) {
        $self->log->error("You need to specify type, user, pass!");
        croak "You need to specify type, user, pass!";
    } # if

    $self->{'bkpType'} = $self->getType(%params);
    
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

    my $dateTime = DateTime->now();
    my $now = $dateTime->ymd('-') . 'T' . $dateTime->hms('-');
    my $aliasBkpDir = $params{'bkpDir'} . '/' . $hostInfo->{'alias'} . '/' . $now;
    my $bkpFileName = $aliasBkpDir . "/" . $now . ".xb." . $compSuffix;
    
    $self->log('base')->info("Creating directory on server for alias $aliasBkpDir");

    mkpath($aliasBkpDir) if ! -d $aliasBkpDir;

    my $fh = IO::File->new($privKeyPath, 'w');
    print $fh $hostInfo->{'priv_key'};
    $fh->close();

    chmod 0600, $privKeyPath;

    my $shell = Term::Shell->new();
    my $result = '';
    
    try{
        $result = $self->{'bkpType'}->rmt_backup(
                                        'hostInfo' => $hostInfo, 
                                        'privKeyPath' => $privKeyPath,
                                        'bkpFileName' => $bkpFileName
                                    );
    } catch {
        File::Path::remove_tree($aliasBkpDir . "/" . $now);
        $self->log->error("Shell command failed! Message: ", $result->{'msg'});
        croak "Shell command failed! Message: " . $result->{'msg'};
    }; # try

    my $lastBkpInfoCmd = "ssh -i " . $privKeyPath . " " . $hostInfo->{'ip'} . " '";
    $lastBkpInfoCmd .= 'mysql -e "select * from PERCONA_SCHEMA.xtrabackup_history';
    $lastBkpInfoCmd .= ' ORDER BY innodb_to_lsn DESC, start_time DESC LIMIT 1"';
    $lastBkpInfoCmd .= ' -u ' . $hostInfo->{'user'} . " -p\Q$hostInfo->{'pass'}\E";
    $lastBkpInfoCmd .= ' -h ' . $hostInfo->{'local_host'} . ' -X';
    $lastBkpInfoCmd .= ' -S ' . $hostInfo->{'socket'} . "'";
    
    $result = $shell->execCmd('cmd' => $lastBkpInfoCmd, 'cmdsNeeded' => [ 'ssh' ]);

    $shell->fatal($result);
    
    $lastBkpInfo = $self->mysqlXmlToHash('xml' => $result->{'msg'});
    
    $lastBkpInfo = $self->bkpInfoTimeToUTC('bkpInfo' => $lastBkpInfo);
    
    $self->log('base')->info("Starting import info about remote backup");

    my @values = values(%$lastBkpInfo);
    my @escVals = map { my $s = $_; $s = $dbh->quote($s); $s } @values;

    $self->log('debug')->debug("Dumping imported info: ", sub { Dumper($lastBkpInfo) });

    $query = "INSERT INTO history(" . join( ",", keys(%$lastBkpInfo) ) . ",";
    $query .=  "bkpconf_id)";
    $query .= " VALUES(" . join( ",", @escVals ). "," . $hostInfo->{'bkpconf_id'} . ")";

    my $sth = $dbh->prepare($query);
    $sth->execute();

    $dbh->disconnect();

    my $uuidFileName = $aliasBkpDir . "/" . $lastBkpInfo->{'uuid'} . ".xb." . $compSuffix;

    $self->log('base')->info("Renaming $bkpFileName to $uuidFileName");

    move($bkpFileName, $uuidFileName);
    
    $self->log('base')->info("Removing temporary private key file");

    unlink $privKeyPath;

    $self->log('base')->info("Backup successful");

} # end sub rmt_backup

=item C<restore>

Method restores backup on host where we execute this method

param:
    
    uuid string - parameter, backup uuid

    bkpDir string - directory where backups will be stored
    
    host string - host name for which we want to execute restore
    
    hostBkpDir string - this is bkpDir plus host, gives directory where backup
                        for specific host is stored
    
    user string - user for mysql database on local backuped host
    
    pass string - password for user
    
    socket string - optional parameter, path to the mysql socket
    
return:

    void
    
=cut

sub restore() {

    my $self = shift;
    my %params = @_;
    my $uuid = $params{'uuid'};
    my $backupsInfo = {};
    
    $self->log('base')->info("Starting local restore of backups with uuid ", $uuid);
    
    my $allBackups = $self->getBackupsInfo(
                                            'user' => $params{'user'},
                                            'pass' => $params{'pass'},
                                            'socket' => $params{'socket'}
                                        );

    $self->log('debug')->debug("Dumping all backups info", , sub { Dumper($allBackups) });

    for my $bkp(@$allBackups) {
        $backupsInfo->{$bkp->{'uuid'}} = $bkp;
    } # for

    if( !defined( $backupsInfo->{$uuid} ) ) {
        $self->log->error("No backups with uuid $uuid!");
        croak "No backups with uuid $uuid!";
    } # if

    if( $backupsInfo->{$uuid}->{'incremental'} eq 'Y' ) {
        $self->{'bkpType'} = $self->getType(
                                            'bkpType' => 'incremental'
                                        );
    } else {
        $self->{'bkpType'} = $self->getType(
                                            'bkpType' => 'full'
                                        );
    } # if
    
    $params{'backupsInfo'} = $backupsInfo;

    $self->{'bkpType'}->restore(%params);

} # end sub restore

=item C<restore_rmt>

param:
    
    uuid string - required parameter, backup uuid
    
    bkpDir string - directory where backups will be stored, on server
    
    host string - host name for which we want to execute restore
    
    hostBkpDir string - this is bkpDir plus host, gives directory where backup
                        for specific host is stored
                        
return:

    return hash_ref - information about restored backup
    
=cut

sub restore_rmt() {

    my $self = shift;
    my %params = @_;
    my $uuid = $params{'uuid'};
    my $backupsInfo = {};

    $self->log('base')->info("Starting remote restore of backups with uuid ", $uuid);

    my $allBackups = $self->getRmtBackupsInfo('uuid' => $uuid);

    $self->log('debug')->debug("Dumping all backups info", , sub { Dumper($allBackups) });

    for my $bkp(@$allBackups) {
        $backupsInfo->{$bkp->{'uuid'}} = $bkp;
    } # for

    if( !defined( $backupsInfo->{$uuid} ) ) {
        $self->log->error("No backups with uuid $uuid!");
        croak "No backups with uuid $uuid!";
    } # if

    my $info = $backupsInfo->{$uuid};

    if( $backupsInfo->{$uuid}->{'incremental'} eq 'Y' ) {
        $self->{'bkpType'} = $self->getType(
                                            'bkpType' => 'incremental'
                                        );
    } else {
        $self->{'bkpType'} = $self->getType(
                                            'bkpType' => 'full'
                                        );
    } # if
    
    $params{'backupsInfo'} = $backupsInfo;

    $self->{'bkpType'}->restore_rmt(%params);

    return $info;

} # end sub restore_rmt

=item C<dump_rmt>

Method dumps one or more databases, which where backed up previously from
remote host

param:
    
    uuid string - required parameter, backup uuid
    
    location string - required parameter, location where db backup will be 
                    restored and dumped
    
    dbname string - required parameter, names of databases to dump, you can pass
                    all - for all databases
    
    user string - user name for mysql user, used to dump passed databases on server
                  from which remote backups are done
    
    pass string - password for user
    
return:

    void
    
=cut

sub dump_rmt() {
    
    my $self = shift;
    my %params = @_;
    my $location = $params{'location'};
    my $databases = $params{'dbname'};
    my @databases = split(",", $databases);
    my $compSuffix = $self->{'compressions'}->{$self->{'compression'}};
    my $compUtil = $self->{'compression'};
    my $timeout = 60;
    
    $self->log('base')->info("Starting remote dump of backup with uuid ", $params{'uuid'});

    my $stopDb = "service mysql stop";
    my $startDb = "mysqld_safe --defaults-file=" . $location . "/backup-my.cnf";
    $startDb .= " --datadir=" . $location;
    
    $self->log('base')->info("Checking mysql status");

    my $shell = Term::Shell->new();
    my $result = '';
    
    if( -S $params{'socket'} ) {
        $self->log('base')->info("Stoping mysql server");
        $result = $shell->execCmd('cmd' => $stopDb, 'cmdsNeeded' => [ 'service' ]);
        $shell->fatal($result);
    } # if

    $self->log('base')->info("Removing $location");

    File::Path::remove_tree($location);

    my $backupInfo = {};

    try {
        $backupInfo = $self->restore_rmt(%params);
    } catch {
        mkpath($location) if ! -d $location;
        my $error = $_ || 'unknown error';
        $self->log->error("Error: ", $error);
        die $error;
    }; # try

    my $stopDbSafe = "mysqladmin -u " . $backupInfo->{'user'} . " -p\Q$backupInfo->{'pass'}\E shutdown";
        
    my $uid = getpwnam('mysql');
    my $gid = getgrnam('mysql');

    $self->log('base')->info("Restoring owners of restored files");

    find(
        sub {
            chown $uid, $gid, $_ or die "could not chown '$_': $!";
        },
        "$location"
    );

    $self->log('base')->info("Starting mysql server with innobackupex .cnf file");

    $result = $shell->execCmd('cmd' => $startDb, 'cmdsNeeded' => [ 'mysqld_safe' ], 'bg' => 1);
    $shell->fatal($result);
    
    $self->log('base')->info("Waiting for server start ", $timeout, ' seconds');
    
    my $loop = 0;
    
    while( $loop <= $timeout ) {
    
        if( -S $params{'socket'} ) {
            last;
        } elsif( $loop == $timeout ) {
            $self->log->error("Failed to start mysql server!");
            croak "Failed to start mysql server!";
        } # if
        
        sleep 1;
        $loop++;
        
    } # while
    
    for my $db(@databases) {

        my $startTime = $backupInfo->{'start_time'};
        $startTime =~ s/\s/T/g;
        $startTime =~ s/\:/-/g;
        my $dbsOpt = '';

        my $dumpDbPath = $location . "/" . $startTime;
        my $dumpDbFile = $dumpDbPath . "/" . $db . "." . $compSuffix;

        if( $db eq 'all' ) {
            $dbsOpt = '--all-databases'
        } else {
            $dbsOpt = '--databases ' . $db;
        } # if

        $self->log('base')->info("Mysql dump of database: ", $db);

        my $dumpDbCmd = "mysqldump --single-transaction " . $dbsOpt;
        $dumpDbCmd .= " -u " . $backupInfo->{'user'} . " -p\Q$backupInfo->{'pass'}\E";
        $dumpDbCmd .= "| " . $compUtil . " -c > " . $dumpDbFile;

        $self->log('base')->info("Creating directory for dump: ", $dumpDbPath);

        mkpath($dumpDbPath) if ! -d $dumpDbPath;

        try {
            $result = $shell->execCmd('cmd' => $dumpDbCmd, 'cmdsNeeded' => [ 'mysqldump', $compUtil ]);
            $shell->fatal($result);
        } catch {
            $self->log->error("Error: ", $result->{'msg'});
            rmtree($dumpDbPath);
            $shell->fatal($result);
        }; # try

    } # for

    $self->log('base')->info("Stopping mysql server");

    my $shell = Term::Shell->new();
    my $result = $shell->execCmd('cmd' => $stopDbSafe, 'cmdsNeeded' => [ 'mysqladmin' ]);

    $shell->fatal($result);
        
    $self->log('base')->info("Dump successful");

} # end sub dump_rmt

=item C<list>

Method lists all local backups in supplied format

param:

    format string - one of supported formats of output

return:

    void
    
=cut

sub list() {

    my $self = shift;
    my %params = @_;
    my $format = $params{'format'};

    # getting information about backups
    my $data = $self->getBackupsInfo(
                                        'user' => $params{'user'},
                                        'pass' => $params{'pass'},
                                        'socket' => $params{'socket'}
                                    );
    
    $self->$format('data' => $data);

} # end sub list

=item C<list_rmt>

Method lists all backups which were done on remote host and transfered on host
executing rmt_backup

param:

    format string - one of supported formats of output

return:

    void
    
=cut

sub list_rmt() {

    my $self = shift;
    my %params = @_;
    my $format = $params{'format'};

    # getting information about backups
    my $data = $self->getRmtBackupsInfo();
    
    $format .= '_rmt';
    $self->$format('data' => $data);

} # end sub list_rmt

=item C<getBackupsInfo>

Method get information about all local backups

param:

    user string - user to get info about backups, this database is local
                  for each backuped host
    
    pass string - password for user
    
    socket string - socket on which database server is running
    
return:

    $backupsInfo array_ref - list of hashes of all backups
    
=cut

sub getBackupsInfo() {

    my $self = shift;
    my %params = @_;

    my @backupsInfo = ();
    
    $self->log('debug')->debug("Getting backups info with params: ", , sub { Dumper(\%params) });

    my $dbh = DBI->connect(
                            "DBI:mysql:database=PERCONA_SCHEMA;host=localhost;mysql_socket=" . $params{'socket'},
                            $params{'user'}, 
                            $params{'pass'},
                            {'RaiseError' => 1}
                        );

    my $query = "SELECT * FROM PERCONA_SCHEMA.xtrabackup_history";
    $query .= " ORDER BY innodb_to_lsn ASC, start_time ASC";

    $self->log('debug')->debug("Query: ", $query);
    
    @backupsInfo = @{ $dbh->selectall_arrayref($query, { Slice => {} }) };

    $dbh->disconnect();

    return \@backupsInfo;

} # end sub getBackupsInfo

=item C<getRmtBackupsInfo>

Method gets information about all remote backups, this info is copied from local
database on remote backuped host and copied after backup and stored in local
database on server where rmt_backup was executed

param:

return:

    $backupsInfo array_ref - list of all backup info
    
=cut

sub getRmtBackupsInfo() {

    my $self = shift;
    my %params = @_;
    my $uuid = $params{'uuid'};
    my @backupsInfo = ();

    $self->log('debug')->debug("Getting remotee backups info with params: ", , sub { Dumper(\%params) });

    my $dbh = DBI->connect(
                            "dbi:SQLite:dbname=" . $self->{'bkpDb'},
                            "", 
                            "",
                            {'RaiseError' => 1}
                        );
    
    my $query = "SELECT * FROM host JOIN bkpconf JOIN history";
    $query .= " ON host.host_id=bkpconf.host_id";
    $query .= " AND bkpconf.bkpconf_id=history.bkpconf_id";
    
    if( $uuid ) {
        my $subQuery = "SELECT bkpconf.bkpconf_id FROM bkpconf JOIN history";
        $subQuery .= " on bkpconf.bkpconf_id=history.bkpconf_id WHERE uuid='" . $uuid . "'";
        $query .= " AND bkpconf.bkpconf_id IN (" . $subQuery .")";
    } # if
    
    $self->log('debug')->debug("Query: ", $query);
    
    @backupsInfo = @{ $dbh->selectall_arrayref($query, { Slice => {} }) };

    $dbh->disconnect();

    return \@backupsInfo;

} # end sub getRmtBackupsInfo

=item C<getType>

Method generates appropriate backup type object on which we execute actions,
factory method

params:

    bkpType string - type of backup
    
    %params - all remaining params, these are passed to produced objects
    
return:

    $object object
    
=cut

sub getType() {

    my $self = shift;
    my %params = @_;
    my $type = $params{'bkpType'};
    my $class = ref $self;

    $type = ucfirst($type);

    my $produceClass = 'Backup::Type::' . $type;

    $self->log('debug')->debug("Producing new instance of type", $produceClass);

    my $module = $produceClass ;
    $produceClass  =~ s/\:\:/\//g;

    require "$produceClass.pm";
    $module->import();

    my $object = $module->new(@_);

    return $object;
	
} # end sub getType

=item C<tbl>

Method outputs data passed in table format

param:

    data array_ref - list of hashes with information
    
return:

    void
    
=cut

sub tbl() {

    my $self = shift;
    my %params = @_;
    my $data = $params{'data'};

    my $bkpTbl = Text::SimpleTable->new(
                                        [19, 'start_time'],
                                        [36, 'uuid'],
                                        [16, 'end_lsn'],
                                        [1, 'p'],
                                        [1, 'i'],
                                        [1, 't' ],
                                        [1, 'c' ]
                                    );
    
    for my $info(@$data) {
        $bkpTbl->row(
                        $info->{'start_time'},
                        $info->{'uuid'},
                        $info->{'innodb_to_lsn'},
                        $info->{'partial'},
                        $info->{'incremental'},
                        $info->{'compact'},
                        $info->{'compressed'}
                    );
        $bkpTbl->hr;
    } # for

    print $bkpTbl->draw;

} # end sub tbl

=item C<lst>

Method outputs data passed in list format

param:

    data array_ref - list of hashes with information
    
return:

    void
    
=cut

sub lst() {

    my $self = shift;
    my %params = @_;
    my $data = $params{'data'};

    my $bkpTbl = Text::SimpleTable->new(
                                        [70, 'Backup Info'],
                                    );
    
    for my $info(@$data) {

        my $row = '';

        while ( my ($key,$value) = each %$info) {
            $row .= $key . ': ' . $value . "\n";
        } # while

        $bkpTbl->row(
                        $row
                    );
        $bkpTbl->hr;

    } # for

    print $bkpTbl->draw;

} # end sub lst

=item C<tbl_rmt>

Method outputs data passed in table format for remote backups

param:

    data array_ref - list of hashes with information
    
return:

    void
    
=cut

sub tbl_rmt() {

    my $self = shift;
    my %params = @_;
    my $data = $params{'data'};

    my $bkpTbl = Text::SimpleTable->new(
                                        [19, 'host_name'],
                                        [19, 'alias'],
                                        [19, 'start_time'],
                                        [36, 'uuid'],
                                        [16, 'end_lsn'],
                                        [1, 'p'],
                                        [1, 'i'],
                                        [1, 't' ],
                                        [1, 'c' ]
                                    );
    
    for my $info(@$data) {
        $bkpTbl->row(
                        $info->{'host_name'},
                        $info->{'alias'},
                        $info->{'start_time'},
                        $info->{'uuid'},
                        $info->{'innodb_to_lsn'},
                        $info->{'partial'},
                        $info->{'incremental'},
                        $info->{'compact'},
                        $info->{'compressed'}
                    );
        $bkpTbl->hr;
    } # for

    print $bkpTbl->draw;

} # end sub tbl_rmt

=item C<lst_rmt>

Method outputs data passed in list format for remote backups

param:

    data array_ref - list of hashes with information
    
return:

    void
    
=cut

sub lst_rmt() {

    my $self = shift;
    my %params = @_;
    
    $self->lst(%params);

} # end sub lst

no Moose::Role;

=head1 AUTHOR

        PAVOL IPOTH <pavol.ipoth@gmail.com>

=head1 COPYRIGHT

        Pavol Ipoth, ALL RIGHTS RESERVED, 2015

=head1 License

        GPLv3

=cut

1;

