package Biblio::Folio::Site::Searcher::ByIdSet;

use strict;
use warnings;

use Biblio::Folio::Site::Searcher;

use vars qw(@ISA);

@ISA = qw(Biblio::Folio::Site::Searcher::ByIdReader);

sub new {
    my $self = shift;
    unshift @_, 'set' if @_ % 2;
    $self->SUPER::new(@_);
}

sub set { @_ > 1 ? $_[0]{'set'} = $_[1] : $_[0]{'set'} }

sub init {
    my ($self) = @_;
    my $set = $self->{'set'};
    die 'internal error: a batch ID searcher requires a set'
        if !defined $set;
    die "internal error: ID set must be an array ref" if ref $set ne 'ARRAY';
    $self->SUPER::init;
}

sub add {
    my $self = shift;
    my $set = $self->set;
    push @$set, @_;
    return $self;
}

sub read_one {
    my ($self) = @_;
    my $set = $self->set;
    return if !@$set;
    return shift @$set;
};

1;
