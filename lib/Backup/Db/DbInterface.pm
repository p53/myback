package Backup::Db::DbInterface;

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

=item bkpDb string

    stores default path to sqlite database, used for storing
    remote backups information
    
=cut

has 'bkpDb' => (
    is => 'rw',
    default => '/var/lib/myback/bkpdb'
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

sub hashInsert {

    my $self = shift;
    my %params = @_;
    my $table = $params{'table'};
    my $data = $params{'data'};

    # inserting info about file uploaded to glacier
    my @values = values(%$data);
    my @escVals = map { my $s = $_; $s = $self->localDbh->quote($s); $s } @values;

    my $query = "INSERT INTO " . $table . " (" . join( ",", keys(%$data) ) . ")";
    $query .= " VALUES(" . join( ",", @escVals ) . ")";

    my $sth = $self->localDbh->prepare($query);
    $sth->execute();
        
} # end sub hashInsert

sub DEMOLISH {
    my $self = shift;
    $self->localDbh->disconnect();  
} # end sub DEMOLISH

no Moose::Role;

1;