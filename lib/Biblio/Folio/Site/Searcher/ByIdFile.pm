package Biblio::Folio::Site::Searcher::ByIdFile;

use strict;
use warnings;

use Biblio::Folio::Site::Searcher::ByIdReader;

use vars qw(@ISA);

@ISA = qw(Biblio::Folio::Site::Searcher::ByIdReader);

sub new {
    my $self = shift;
    if (@_ % 2) {
        unshift @_, ref $_[0] ? 'fh' : 'file';
    }
    $self->SUPER::new(@_);
}

sub file { @_ > 1 ? $_[0]{'file'} = $_[1] : $_[0]{'file'} }
sub fh { @_ > 1 ? $_[0]{'fh'} = $_[1] : $_[0]{'fh'} }

sub init {
    my ($self) = @_;
    my ($file, $fh) = @$self{qw(file fh)};
    if (!defined $fh) {
        die 'internal error: a batch ID searcher requires a file to read IDs from'
            if !defined $file;
        open my $fh, '<', $file or die "open $file: $!";
        $self->SUPER::init;
        $self->{'fh'} = $fh;
    }
}

sub read_one {
    my ($self) = @_;
    my $fh = $self->fh;
    my $id = <$fh>;
    return if !defined $id;
    chomp $id;
    return $id;
};

1;
