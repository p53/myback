package Backup::Backup;

use Moose;
use namespace::autoclean;
use Carp;
use Try::Tiny;
use warnings;
use autodie;
use POSIX;
use Text::SimpleTable;

with 'Backup::BackupInterface';

sub backup() {

    my $self = shift;
    my %params = @_;

    if( !( defined $params{'bkpType'} && defined $params{'user'} && defined $params{'port'} && defined $params{'pass'} ) ) {
        croak "You need to specify type, user, port, pass!";
    } # if

    $self->{'bkpType'} = $self->getType(%params);

    $self->{'bkpType'}->backup(%params);
    
} # end sub backup

sub restore() {

    my $self = shift;
    my %params = @_;
    my $uuid = $params{'uuid'};

    my $backupsInfo = $self->getBackupsInfo();

    if( !defined( $backupsInfo->{$uuid} ) ) {
        croak "No backups with uuid $uuid!";
    } # if

    if( $backupsInfo->{$uuid}->{'incremental'} eq 'Y' ) {
        $self->{'bkpType'} = $self->getType(
                                            'bkpType' => 'incremental',
                                            'bkpDir' => $self->{'bkpDir'}, 
                                            'host' => $self->{'host'},
                                            'hostBkpDir' => $self->{'hostBkpDir'}
                                        );
    } else {
        $self->{'bkpType'} = $self->getType(
                                            'bkpType' => 'full',
                                            'bkpDir' => $self->{'bkpDir'}, 
                                            'host' => $self->{'host'},
                                            'hostBkpDir' => $self->{'hostBkpDir'}
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
    my $backupsInfo = $self->getBackupsInfo();

    # sorting backup info according backup start time
    my @sortedBkpsInfo = sort { $a->{'start_unix_time'} <=> $b->{'start_unix_time'} } values %$backupsInfo;

    $self->$format('data' => \@sortedBkpsInfo);

} # end sub list

sub getBackupsInfo() {

    my $self = shift;
    my %params = @_;

    my $hostBkpDir = $self->{'hostBkpDir'};
    my %backupsInfo = ();

    # getting files with lsn number from all backups in host backup directory 
    my @sources = <$hostBkpDir/*/xtrabackup_info>;

    # collecting backups info
    for my $file(@sources) {

        my $fh = IO::File->new();
        $fh->open("< $file");
        my @lines = <$fh>;
        $fh->close();

        my $backupInfo = {};
        my $uuid = '';

        $file =~ /(.*)\/.*$/;
        $backupInfo->{'bkpDir'} = $1;

        # parsing file
        for my $line(@lines) {

            if( $line =~ /([a-zA-Z0-9\_]+)\s+=\s+(.*)$/ ) {

                my $prop = $1;
                my $propVal = $2;

                $backupInfo->{$prop} = $propVal;

                # we need to convert date to timestamp, to order results
                # according timestamp
                if( $prop eq 'start_time' ) {

                    my $date = $propVal;
                    my @dateParts = split(" ", $date);
                    my ($year, $month, $day) = split("-", $dateParts[0]);
                    my ($hour, $minute, $sec) = split(":", $dateParts[1]);
                    my $unixTime = mktime($sec, $minute, $hour, $day, $month, $year, 0, 0);

                    $backupInfo->{'start_unix_time'} = $unixTime;

                } # if

                if( $prop eq 'uuid' ) {
                    $uuid = $propVal;
                } # if

            } # if

        } # for

        $backupsInfo{$uuid} = $backupInfo;
 
    } # for

    return \%backupsInfo;

} # end sub getBackupsInfo

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

no Moose::Role;

1;

