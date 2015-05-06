package Backup::BackupInterface;

use Moose::Role;
use namespace::autoclean;
use Moose::Util::TypeConstraints;

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
    isa => 'Str',
    required => 1
);

has 'host' => (
    is => 'rw',
    isa => 'Str',
    required => 1
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

has 'date' => (
    is => 'rw',
    isa => 'DateTime'
);

has 'location' => (
    is => 'rw',
    isa => 'Str'
);

has 'dbname' => (
    is => 'rw',
    isa => 'Str'
);

sub backup() {}

sub restore() {}

sub dump() {}

no Moose::Role;

1;
