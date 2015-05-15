package Backup::Backup;

use Moose;
use namespace::autoclean;
use Carp;
use Try::Tiny;
use warnings;
use autodie;
use POSIX;
use Text::SimpleTable;
use DBI;

with 'Backup::BackupInterface';

sub backup() {

    my $self = shift;
    my %params = @_;

    if( !( defined $params{'bkpType'} && defined $params{'user'} && defined $params{'pass'} ) ) {
        croak "You need to specify type, user, pass!";
    } # if

    $self->{'bkpType'} = $self->getType(%params);

    $self->{'bkpType'}->backup(%params);
    
} # end sub backup

sub rmt_backup() {

    my $self = shift;
    my %params = @_;

    if( !( defined $params{'bkpType'} && defined $params{'user'} && defined $params{'pass'} ) ) {
        croak "You need to specify type, user, pass!";
    } # if

    $self->{'bkpType'} = $self->getType(%params);

    $self->{'bkpType'}->rmt_backup(%params);

} # end sub rmt_backup

sub restore() {

    my $self = shift;
    my %params = @_;
    my $uuid = $params{'uuid'};
    my $backupsInfo = {};
    my $allBackups = $self->getBackupsInfo();

    for my $bkp(@$allBackups) {
        $backupsInfo->{$bkp->{'uuid'}} = $bkp;
    } # for

    if( !defined( $backupsInfo->{$uuid} ) ) {
        croak "No backups with uuid $uuid!";
    } # if

    if( $backupsInfo->{$uuid}->{'incremental'} eq 'Y' ) {
        $self->{'bkpType'} = $self->getType(
                                            'bkpType' => 'incremental',
                                            'bkpDir' => $self->{'bkpDir'}, 
                                            'host' => $self->{'host'},
                                            'hostBkpDir' => $self->{'hostBkpDir'},
                                            'user' => $self->{'user'},
                                            'pass' => $self->{'pass'},
                                            'socket' => $self->{'socket'}
                                        );
    } else {
        $self->{'bkpType'} = $self->getType(
                                            'bkpType' => 'full',
                                            'bkpDir' => $self->{'bkpDir'}, 
                                            'host' => $self->{'host'},
                                            'hostBkpDir' => $self->{'hostBkpDir'},
                                            'user' => $self->{'user'},
                                            'pass' => $self->{'pass'},
                                            'socket' => $self->{'socket'}
                                        );
    } # if
    
    $params{'backupsInfo'} = $backupsInfo;

    $self->{'bkpType'}->restore(%params);

} # end sub restore

sub dump() {
    my $self = shift;
    $self->{'bkpType'}->dump();
}

sub list() {

    my $self = shift;
    my %params = @_;
    my $format = $params{'format'};

    # getting information about backups
    my $data = $self->getBackupsInfo();
    
    $self->$format('data' => $data);

} # end sub list

sub list_rmt() {

    my $self = shift;
    my %params = @_;
    my $format = $params{'format'};

    # getting information about backups
    my $data = $self->getRmtBackupsInfo();
    
    $format .= '_rmt';
    $self->$format('data' => $data);

} # end sub list_rmt

sub getBackupsInfo() {

    my $self = shift;
    my %params = @_;

    my @backupsInfo = ();

    my $dbh = DBI->connect(
                            "DBI:mysql:database=PERCONA_SCHEMA;host=localhost;mysql_socket=" . $self->{'socket'},
                            $self->{'user'}, 
                            $self->{'pass'},
                            {'RaiseError' => 1}
                        );

    my $query = "SELECT * FROM PERCONA_SCHEMA.xtrabackup_history";
    $query .= " ORDER BY innodb_to_lsn ASC, start_time ASC";

    @backupsInfo = @{ $dbh->selectall_arrayref($query, { Slice => {} }) };

    $dbh->disconnect();

    return \@backupsInfo;

} # end sub getBackupsInfo

sub getRmtBackupsInfo() {

    my $self = shift;
    my %params = @_;

    my @backupsInfo = ();

    my $dbh = DBI->connect(
                            "dbi:SQLite:dbname=" . $self->{'bkpDb'},
                            "", 
                            "",
                            {'RaiseError' => 1}
                        );

    my $query = "SELECT * FROM host JOIN bkpconf JOIN history";
    $query .= " ON host.host_id=bkpconf.host_id";
    $query .= " AND bkpconf.bkpconf_id=history.bkpconf_id";

    @backupsInfo = @{ $dbh->selectall_arrayref($query, { Slice => {} }) };

    $dbh->disconnect();

    return \@backupsInfo;

} # end sub getRmtBackupsInfo

sub findBkpBy() {

    my $self = shift;
    my %params = @_;
    my $info = ();
    my $cond = '';

    my $dbh = DBI->connect(
                        "DBI:mysql:database=PERCONA_SCHEMA;host=localhost;mysql_socket=" . $self->{'socket'},
                        $self->{'user'}, 
                        $self->{'pass'},
                        {'RaiseError' => 1}
                    );
                    
    for my $key(keys %params) {
        $cond .= " " . $key . '=' . $params{$key} . " AND";
    } # for

    $cond =~ s/(.*)AND$/$1/;

    my $query = "SELECT * FROM PERCONA_SCHEMA.xtrabackup_history";
    $query .= " WHERE " . $cond;

    my @backupsInfo = @{ $dbh->selectall_arrayref($query, { Slice => {} }) };

    $dbh->disconnect();

    return $backupsInfo[0];

} # end sub findBkpBy

sub getType() {

	my $self = shift;
	my %params = @_;
	my $type = $params{'bkpType'};
	my $class = ref $self;
	
	$type = ucfirst($type);
	
	my $produceClass = 'Backup::Type::' . $type;
	
	my $module = $produceClass ;
	$produceClass  =~ s/\:\:/\//g;
	
	require "$produceClass.pm";
	$module->import();
	
	my $object = $module->new(@_);
	
	return $object;
	
} # end sub getType

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

sub lst_rmt() {

    my $self = shift;
    my %params = @_;
    
    $self->lst(%params);

} # end sub lst

no Moose::Role;

1;

