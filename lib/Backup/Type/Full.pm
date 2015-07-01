package Backup::Type::Full;

=head1 NAME

    Backup::Type::Full - one of backup types, serves for executing operations
                         on/with backups

=head1 SYNOPSIS

    my $backupTypeObj = Backup::Type::Full->new();
    
    $backupTypeObj->backup(     
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
use File::Glob;
use File::Copy;
use File::Path;
use File::Basename;
use DateTime;
use Data::Dumper;
use DBI;
use YAML::Tiny;
use File::stat;

use Term::Shell;

with 'Backup::BackupInterface', 'MooseX::Log::Log4perl';

=head1 METHODS

=over 12

=item C<backup>

Method creates full backup on local host

param:

    user string - mysql user for local mysql database with bkp history
    pass string - mysql password for mysql user
    socket string - path to local mysql server
    host string - host name of local mysql server
    hostBkpDir string - directory where backup will be stored on local host
    
return:

    void
    
=cut

sub backup {

    my $self       = shift;
    my %params     = @_;
    my $compSuffix = $self->{'compressions'}->{ $self->{'compression'} };
    my $compUtil   = $self->{'compression'};

    if ( !( defined $params{'user'} && defined $params{'pass'} ) ) {
        $self->log->error("You need to specify user, pass!");
        croak "You need to specify user, pass!";
    } # if

    my $dateTime = DateTime->now();
    my $now      = $dateTime->ymd('-') . 'T' . $dateTime->hms('-');
    my $bkpDir   = $params{'hostBkpDir'} . "/" . $now;

    $self->log('base')->info( "Creating backup directory for local backup:", $bkpDir );

    mkpath($bkpDir) if !-d $bkpDir;

    my $bkpFileName = $bkpDir . "/" . $now . ".xb." . $compSuffix;

    # creating backup on host
    my $bkpCmd = "innobackupex --user=" . $params{'user'};
    $bkpCmd .= " --history --stream=xbstream --host=" . $params{'host'};
    $bkpCmd .= " --password='$params{'pass'}' " . $params{'hostBkpDir'};
    $bkpCmd .= " --socket=" . $params{'socket'};
    $bkpCmd .= "| " . $compUtil . " > " . $bkpFileName;

    $self->log('base')->info("Backing up");

    my $shell  = Term::Shell->new();
    my $result = '';

    try {
        $result = $shell->execCmd(
            'cmd'        => $bkpCmd,
            'cmdsNeeded' => [ 'innobackupex', $compUtil ]
        );
        $shell->fatal($result);
    }
    catch {
        File::Path::remove_tree( $bkpDir . "/" . $now );
        $self->log->error( "Shell command failed! Message: ", $result->{'msg'} );
        croak "Shell command failed! Message: " . $result->{'msg'};
    }; # try

    $self->log('base')->info("Full backup of host $params{'host'} to $params{'hostBkpDir'} on socket $params{'socket'} to file $bkpFileName successful");

    # getting information about backup, innobackupex after successful backup
    # saves information on host in mysql db
    my $lastBkpInfo = $self->getLastBkpInfo(
        'user'   => $params{'user'},
        'pass'   => $params{'pass'},
        'socket' => $params{'socket'}
    );

    $self->log('debug')->debug( "Dumping last backup info: ", sub { Dumper($lastBkpInfo) } );

    # to be able simply find backup during restore, we give it name with uuid
    my $uuidFileName = $bkpDir . "/" . $lastBkpInfo->{'uuid'} . ".xb." . $compSuffix;
    my $uuidConfFile = $bkpDir . "/" . $lastBkpInfo->{'uuid'} . ".yaml";

    $self->log('base')->info("Renaming $bkpFileName to $uuidFileName");

    move( $bkpFileName, $uuidFileName );

    my $filesize = stat($uuidFileName)->size;

    # to have all info in one format we convert time to UTC wherever we are doing
    # backups
    $lastBkpInfo = $self->bkpInfoTimeToUTC( 'bkpInfo' => $lastBkpInfo );
    $lastBkpInfo->{'bkp_size'} = $filesize;

    $self->log('debug')->debug( "Dumping last backup info with UTC times: ", sub { Dumper($lastBkpInfo) } );
    $self->log('base')->info("Writing YAML config for remote backups");

    # storing information about backup also in yml file
    # this is used in rmt_tmp_backup
    my $yaml = YAML::Tiny->new($lastBkpInfo);
    $yaml->write($uuidConfFile);

    $self->log('base')->info("Local backup finished!");

} # end sub backup

=item C<restore>

Restores full backup stored local host

param:

    uuid string - uuid of restored backup
    
    location string - where we want to restore backup
    
    hostBkpDir string - where we store backup
    
return:

=cut

sub restore {

    my $self            = shift;
    my %params          = @_;
    my $uuid            = $params{'uuid'};
    my $restoreLocation = $params{'location'};
    my $compSuffix      = $self->{'compressions'}->{ $self->{'compression'} };
    my $compUtil        = $self->{'compression'};
    my $result          = {};

    if ( !-d $restoreLocation ) {
        $self->log('base')->info("Creating restore directory $restoreLocation");
        mkpath($restoreLocation);
    } # if

    # finding backup with specified uuid
    my @files = glob( $params{'hostBkpDir'} . "/*/" . $uuid . ".xb." . $compSuffix );
    my $bkpFile = $files[0];

    if ( !-f $bkpFile ) {
        $self->log->error("Cannot find file with uuid $uuid!");
        croak "Cannot find file with uuid $uuid!";
    }# if

    $self->log('base')->info("Decompressing backup $bkpFile to $restoreLocation");

    my $shell = Term::Shell->new();

    my $decompCmd = $compUtil . " -c -d " . $bkpFile . " | xbstream -x -C " . $restoreLocation;

    $result = $shell->execCmd(
        'cmd'        => $decompCmd,
        'cmdsNeeded' => [ $compUtil, 'xbstream' ]
    );

    $shell->fatal($result);

    $self->log('base')->info("Applying innodb log and reverting uncommitted transactions to $restoreLocation");

    # during restore we need copy tables but also apply log created during backup
    my $restoreCmd = "innobackupex --apply-log " . $restoreLocation;

    try {
        $result = $shell->execCmd(
            'cmd'        => $restoreCmd,
            'cmdsNeeded' => ['innobackupex']
        );
    }
    catch {
        $self->log->error( "Error: ", $result->{'msg'} );
        remove_tree($restoreLocation);
        $shell->fatal($result);
    }; # try

    $self->log('base')->info("Removing percona files in $restoreLocation");

    unlink glob("$restoreLocation/xtrabackup_*");
    # we don't remove this because we might it need for server start
    #unlink "$restoreLocation/backup-my.cnf";

    $self->log('base')->info("Restoration successful");

} # end sub restore

=item C<rmt_backup>

param:

    hostInfo hash_ref - hash info about remote host
    
    privKeyPath string - private key for remote backuped host
    
    bkpFileName string - name of backup file to which we should store backup
                         from remote host

return:

    result hash_ref - hash info from executed command with message and return code
    
=cut

sub rmt_backup {

    my $self        = shift;
    my %params      = @_;
    my $hostInfo    = $params{'hostInfo'};
    my $privKeyPath = $params{'privKeyPath'};
    my $bkpFileName = $params{'bkpFileName'};
    my $compUtil    = $self->{'compression'};

    my $shell = Term::Shell->new();

    $self->log('base')->info("Executing full backup on remote host $hostInfo->{'ip'} on socket $hostInfo->{'socket'}");

    # we are redirecting errors, because innobackupex displays messages on
    # stderr and it would be without this in our stream, thus corrupting backup
    my $rmtBkpCmd = "ssh -i " . $privKeyPath . " " . $hostInfo->{'ip'} . " '";
    $rmtBkpCmd .= "innobackupex --user=" . $hostInfo->{'user'};
    $rmtBkpCmd .= " --history --stream=xbstream --host=" . $hostInfo->{'local_host'};
    $rmtBkpCmd .= " --password=\Q$hostInfo->{'pass'}\E " . $hostInfo->{'local_dir'};
    $rmtBkpCmd .= " --socket=" . $hostInfo->{'socket'};
    $rmtBkpCmd .= " 2>/dev/null | " . $compUtil . " -c ' > " . $bkpFileName;

    my $result = $shell->execCmd( 'cmd' => $rmtBkpCmd, 'cmdsNeeded' => ['ssh'] );

    $self->log('debug')->debug( "Result of command is: ", $result->{'msg'} );

    $shell->fatal($result);

    # getting information about remote backup from remote host
    my $lastBkpInfoCmd = "ssh -i " . $privKeyPath . " " . $hostInfo->{'ip'} . " '";
    $lastBkpInfoCmd .= 'mysql -e "select * from PERCONA_SCHEMA.xtrabackup_history';
    $lastBkpInfoCmd .= ' ORDER BY innodb_to_lsn DESC, start_time DESC LIMIT 1"';
    $lastBkpInfoCmd .= ' -u ' . $hostInfo->{'user'} . " -p\Q$hostInfo->{'pass'}\E";
    $lastBkpInfoCmd .= ' -h ' . $hostInfo->{'local_host'} . ' -X';
    $lastBkpInfoCmd .= ' -S ' . $hostInfo->{'socket'} . "'";
    
    $result = $shell->execCmd('cmd' => $lastBkpInfoCmd, 'cmdsNeeded' => [ 'ssh' ]);

    $shell->fatal($result);
    
    my $lastBkpInfo = $self->mysqlXmlToHash('xml' => $result->{'msg'});
    $lastBkpInfo = $self->bkpInfoTimeToUTC('bkpInfo' => $lastBkpInfo);

    my $filesize = stat($bkpFileName)->size;
    $lastBkpInfo->{'bkp_size'} = $filesize;
    
    $self->log('base')->info("Starting import info about remote backup");

    # inserting info about backup to backup server database
    my @values = values(%$lastBkpInfo);
    my @escVals = map { my $s = $_; $s = $self->localDbh->quote($s); $s } @values;

    $self->log('debug')->debug("Dumping imported info: ", sub { Dumper($lastBkpInfo) });

    my $query = "INSERT INTO history(" . join( ",", keys(%$lastBkpInfo) ) . ",";
    $query .=  "bkpconf_id)";
    $query .= " VALUES(" . join( ",", @escVals ). "," . $hostInfo->{'confId'} . ")";

    my $sth = $self->localDbh->prepare($query);
    $sth->execute();

    return $lastBkpInfo;
    
} # end sub rmt_backup

=item C<restore_rmt>

Method for restoring remote host backups, stored on server

param:

    %params - all params required by proxied method
    
return:

    void

=back

=cut

sub restore_rmt {

    my $self   = shift;
    my %params = @_;

    $self->restore(%params);

} # end sub restore_rmt

no Moose::Role;

=head1 AUTHOR

        PAVOL IPOTH <pavol.ipoth@gmail.com>

=head1 COPYRIGHT

        Pavol Ipoth, ALL RIGHTS RESERVED, 2015

=head1 License

        GPLv3

=cut

1;
