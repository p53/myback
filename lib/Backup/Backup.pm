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
use File::stat;

use Term::Shell;

with 'Backup::BackupInterface', 
     'MooseX::Log::Log4perl';

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

sub backup {

    my $self = shift;
    my %params = @_;

    $self->log('base')->info("Starting local backup");

    if( !( defined $params{'bkpType'} ) ) {
        $self->log('base')->error("You need to specify type!");
        croak "You need to specify type!";
    } # if

    my $optiCmd = $self->getOptimizeCmd(%params);
    
    my $shell = Term::Shell->new();
    my $result = '';
    
    if( $params{'optimize'} eq 'yes' ) {
    
        $self->log('base')->info("Optimizing databases");
        
        try {
            $result = $shell->execCmd(
                'cmd'        => $optiCmd,
                'cmdsNeeded' => [ 'mysqlcheck' ]
            );
            $shell->fatal($result);
        }
        catch {
            $self->log('base')->error( "Shell command failed! Message: ", $result->{'msg'} );
            croak "Shell command failed! Message: " . $result->{'msg'};
        }; # try
        
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

sub rmt_backup {

    my $self = shift;
    my %params = @_;
    my $privKeyPath = '/tmp/' . $params{'host'} . '.priv';
    my $hostInfo = {};
    my @hostsInfo = ();
    my $lastBkpInfo = {};
    my $compSuffix = $self->{'compressions'}->{$self->{'compression'}};
    my $compUtil = $self->{'compression'};
    my $shell = Term::Shell->new();
    my $result = '';
    
    $self->log('base')->info("Starting remote backup");

    if( !( defined $params{'user'} && defined $params{'pass'} ) ) {
        $self->log('base')->error("You need to specify user, pass!");
        croak "You need to specify type, user, pass!";
    } # if

    $self->{'bkpType'} = $self->getType(%params);
    
    $self->log('base')->info("Starting remote backup for host alias ", $params{'host'});

    # getting information about host from db
    my $query = "select *, bkpconf.bkpconf_id AS confId from host JOIN bkpconf";
    $query .= " ON host.host_id=bkpconf.host_id LEFT JOIN history";
    $query .= " ON history.bkpconf_id=bkpconf.bkpconf_id WHERE bkpconf.alias='" . $params{'host'} . "'";
    $query .= " ORDER BY innodb_to_lsn DESC, start_time DESC LIMIT 1";
    
    $self->log('debug')->debug("Query: ", , sub { Dumper($query) });
    
    try {
        @hostsInfo = @{ $self->localDbh->selectall_arrayref($query, { Slice => {} }) };
    } catch {
        my $error = @_ || $_;
        $self->log('base')->error("Error: ", $error, " Query: " . $query);
        croak "Error: " . $error;
    };
    
    if( scalar(@hostsInfo) == 0 ) {
        $self->log('base')->error("No such host!");
        croak "No such host!";
    } elsif( scalar(@hostsInfo) > 1 ) {
        $self->log('base')->error("Found more than one alias with that name, check your DB!");
        croak "Found more than one alias with that name, check your DB!";
    } # if

    $hostInfo = $hostsInfo[0];

    if( !( defined $hostInfo->{'user'} && defined $hostInfo->{'pass'} ) ) {
        $self->log('base')->error("You need to specify user, pass for remote host!");
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

    if( $params{'optimize'} eq 'yes' ) {
    
        $self->log('base')->info("Optimizing databases");

        my $optiCmd = $self->getOptimizeCmd(%$hostInfo);
        my $remoteOptiCmd = "ssh -i " . $privKeyPath . " " . $hostInfo->{'ip'} . " '";
        $remoteOptiCmd .= $optiCmd . "'";

        try {
            $result = $shell->execCmd( 'cmd' => $remoteOptiCmd, 'cmdsNeeded' => ['ssh', 'mysqlcheck'] );
            $self->log('debug')->debug( "Result of command is: ", $result->{'msg'} );
            $shell->fatal($result);
        } catch {
            $self->log('base')->error("Error while executing command, message: ", $result->{'msg'});
            croak "Error while executing command, message: " . $result->{'msg'};
        }; # try

    } # if
     
    $lastBkpInfo = $self->{'bkpType'}->rmt_backup(
                                    'hostInfo' => $hostInfo, 
                                    'privKeyPath' => $privKeyPath,
                                    'bkpFileName' => $bkpFileName
                                );
    
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
    
    user string - user for mysql database on local backuped host
    
    pass string - password for user
    
    socket string - path to the mysql socket
    
    %params - all remaining params, these are passed to produced objects
    
return:

    void
    
=cut

sub restore {

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
        $self->log('base')->error("No backups with uuid $uuid!");
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
    
    %params - all remaining params, these are passed to produced objects
     
return:

    return hash_ref - information about restored backup
    
=cut

sub restore_rmt {

    my $self = shift;
    my %params = @_;
    my $uuid = $params{'uuid'};

    $self->log('base')->info("Starting remote restore of backups with uuid ", $uuid);

    # getting info about remotly backuped backup from server db
    my $backups = $self->getRmtBackupsInfo('uuid' => $uuid);

    $self->log('debug')->debug("Dumping backups info", , sub { Dumper($backups) });

    if( scalar(@$backups) == 0 ) {
        $self->log('base')->error("No backups with uuid $uuid!");
        croak "No backups with uuid $uuid!";
    } # if

    # if our backup is incremental start restore procedure specific for
    # incremental otherwise for full
    if( $backups->[0]->{'incremental'} eq 'Y' ) {
        $self->{'bkpType'} = $self->getType(
                                            'bkpType' => 'incremental'
                                        );
    } else {
        $self->{'bkpType'} = $self->getType(
                                            'bkpType' => 'full'
                                        );
    } # if

    $self->{'bkpType'}->restore_rmt(%params);

    return $backups->[0];

} # end sub restore_rmt

=item C<dump_rmt>

Method dumps one or more databases, which where backed up previously from
remote host

param:
    
    uuid string - backup uuid
    
    location string - location where db backup will be 
                    restored and dumped
    
    dbname string - names of databases to dump, you can pass
                    all - for all databases

    socket string - socket of mysql server to which we are restoring backup
    
return:

    void
    
=cut

sub dump_rmt {
    
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
    
    # we are checking if some server is running by checking socket file existence
    # we also assume that on backup server mysql will be run in default mode
    # so service command will work, we are stopping mysql server
    if( -S $params{'socket'} ) {
        $self->log('base')->info("Stoping mysql server");
        
        try {
            $result = $shell->execCmd('cmd' => $stopDb, 'cmdsNeeded' => [ 'service' ]);
            $shell->fatal($result);
        } catch {
            $self->log('base')->error("Error while executing command, message: ", $result->{'msg'});
            croak "Error while executing command, message: " . $result->{'msg'};
        };
        
    } # if

    $self->log('base')->info("Removing $location");

    # we remove all files from location where we want to restore
    File::Path::remove_tree($location);

    my $backupInfo = {};

    # restoring backup
    try {
        $backupInfo = $self->restore_rmt(%params);
    } catch {
        mkpath($location) if ! -d $location;
        my $error = $_ || 'unknown error';
        $self->log('base')->error("Error: ", $error);
        die $error;
    }; # try

    my $stopDbSafe = "mysqladmin -u " . $backupInfo->{'user'} . " -p\Q$backupInfo->{'pass'}\E shutdown";
    
    # we change owner of restored files
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

    # start our own mysql instance with innobackupex cnf config file,
    # we do it because some parameters on remote host might be different
    # from backup server and thus preventing start and dump on backup server
    try {
        $result = $shell->execCmd('cmd' => $startDb, 'cmdsNeeded' => [ 'mysqld_safe' ], 'bg' => 1);
        $shell->fatal($result);
    } catch {
        $self->log('base')->error("Error while executing command, message: ", $result->{'msg'});
        croak "Error while executing command, message: " . $result->{'msg'};
    };
        
    $self->log('base')->info("Waiting for server start ", $timeout, ' seconds');
    
    my $loop = 0;
    
    while( $loop <= $timeout ) {
    
        if( -S $params{'socket'} ) {
            last;
        } elsif( $loop == $timeout ) {
            $self->log('base')->error("Failed to start mysql server!");
            croak "Failed to start mysql server!";
        } # if
        
        sleep 1;
        $loop++;
        
    } # while
    
    # we are dumping each database passed or all databases
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
            $self->log('base')->error("Error: ", $result->{'msg'});
            rmtree($dumpDbPath);
            $shell->fatal($result);
        }; # try

    } # for

    $self->log('base')->info("Stopping mysql server");

    # after dump we are stopping database server, because otherwise we would need
    # to stop it next time, but next time we could not do it with service command
    # because we started it with mysqld_safe and with mysqladmin we could not
    # stop it either because we would not know mysql root password of previously 
    # restored backup
    try {
        $shell = Term::Shell->new();
        $result = $shell->execCmd('cmd' => $stopDbSafe, 'cmdsNeeded' => [ 'mysqladmin' ]);
    } catch {
        $self->log('base')->error("Error while executing command, message: ", $result->{'msg'});
        croak "Error while executing command, message: " . $result->{'msg'};
    };
    
    $shell->fatal($result);
        
    $self->log('base')->info("Dump successful");

} # end sub dump_rmt

=item C<list>

Method lists all local backups in supplied format

param:

    user string - mysql user of local database
    
    pass string - mysql password for user
    
    socket string - socket for local database server
    
    format string - one of supported formats of output

return:

    void
    
=cut

sub list {

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

sub list_rmt {

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

sub getBackupsInfo {

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
    
    try {
        @backupsInfo = @{ $dbh->selectall_arrayref($query, { Slice => {} }) };
    } catch {
        my $error = @_ || $_;
        $self->log('base')->error("Error: ", $error, " Query: " . $query);
        croak "Error: " . $error;
    }; # try
    
    $dbh->disconnect();

    return \@backupsInfo;

} # end sub getBackupsInfo

=item C<getRmtBackupsInfo>

Method gets information about all remote backups, this info is copied from local
database on remote backuped host and copied after backup and stored in local
database on server where rmt_backup was executed

param:

    uuid string - backup uuid, optional
    
return:

    $backupsInfo array_ref - list of backup info for specific uuid or all backups
                             if not specified
    
=cut

sub getRmtBackupsInfo {

    my $self = shift;
    my %params = @_;
    my $uuid = $params{'uuid'};
    my @backupsInfo = ();

    $self->log('debug')->debug("Getting remote backups info with params: ", , sub { Dumper(\%params) });
    
    # we are selecting all backup info about all backups or backups which
    # have same bkpconf_id as was our uuid backup, thus belonging to same alias
    my $query = "SELECT * FROM host JOIN bkpconf JOIN history";
    $query .= " ON host.host_id=bkpconf.host_id";
    $query .= " AND bkpconf.bkpconf_id=history.bkpconf_id";
    
    if( $uuid ) {
        $query .= " WHERE uuid='" . $uuid . "'";
    } # if
    
    $self->log('debug')->debug("Query: ", $query);
    
    try {
        @backupsInfo = @{ $self->localDbh->selectall_arrayref($query, { Slice => {} }) };
    } catch {
        my $error = @_ || $_;
        $self->log('base')->error("Error: ", $error, " Query: " . $query);
        croak "Error: " . $error;
    };
    
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

sub getType {

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

sub tbl {

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

sub lst {

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

sub tbl_rmt {

    my $self = shift;
    my %params = @_;
    my $data = $params{'data'};

    my @units = ('b','Kb','Mb','Gb','Tb','Pb','Eb');
    
    my $bkpTbl = Text::SimpleTable->new(
                                        [19, 'alias'],
                                        [19, 'start_time'],
                                        [36, 'uuid'],
                                        [10, 'bkp_size'],
                                        [1, 'p'],
                                        [1, 'i'],
                                        [1, 't' ],
                                        [1, 'c' ]
                                    );
    
    my $sum = 0;
    
    for my $info(@$data) {
    
        my $converted = $self->prettySize( 'size' => $info->{'bkp_size'} );
        
        $bkpTbl->row(
                        $info->{'alias'},
                        $info->{'start_time'},
                        $info->{'uuid'},
                        $converted,
                        $info->{'partial'},
                        $info->{'incremental'},
                        $info->{'compact'},
                        $info->{'compressed'}
                    );
        $bkpTbl->hr;
        
        $sum += $info->{'bkp_size'};
        
    } # for

    my $convSum = $self->prettySize( 'size' => $sum );
    
    $bkpTbl->row('','','',$convSum,'','','','');
    $bkpTbl->hr;
    
    print $bkpTbl->draw;

} # end sub tbl_rmt

=item C<lst_rmt>

Method outputs data passed in list format for remote backups

param:

    data array_ref - list of hashes with information
    
return:

    void
    
=back

=cut

sub lst_rmt {

    my $self = shift;
    my %params = @_;
    
    $self->lst(%params);

} # end sub lst

sub getOptimizeCmd {

    my $self = shift;
    my %params = @_;
    my $user = $params{'user'};
    my $pass = $params{'pass'};
    my $socket = $params{'socket'};
    
    my $cmd = "mysqlcheck -u " . $user . " -p\Q${pass}\E" . " -S " . $socket;
    $cmd .= " --optimize --all-databases";

    return $cmd;
    
} # end sub getOptimizeCmd

no Moose::Role;

=head1 AUTHOR

        PAVOL IPOTH <pavol.ipoth@gmail.com>

=head1 COPYRIGHT

        Pavol Ipoth, ALL RIGHTS RESERVED, 2015

=head1 License

        GPLv3

=cut

1;

