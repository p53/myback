package Backup::Type::Full;

use Moose;
use namespace::autoclean;
use Carp;
use Try::Tiny;
use warnings;
use autodie;

use Term::Shell;

with 'Backup::BackupInterface';

sub backup() {

    my $self = shift;
    my %params = @_;

    if( !( defined $params{'user'} && defined $params{'port'} && defined $params{'pass'} ) ) {
        croak "You need to specify user, port, pass!";
    }

    mkdir $self->{'hostBkpDir'} if ! -d $self->{'hostBkpDir'};

    my $fullBkpCmd = "innobackupex --user=" . $self->{'user'};
    $fullBkpCmd .= " --compress --host=" . $self->{'host'};
    $fullBkpCmd .= " --password=" . $self->{'pass'} . " " . $self->{'hostBkpDir'};

    my $shell = Term::Shell->new();
    my $result = $shell->execCmd('cmd' => $fullBkpCmd, 'cmdsNeeded' => [ 'innobackupex' ]);

    $shell->fatal($result);

}

sub restore() {
}

sub dump() {
}

no Moose::Role;

1;