package Biblio::Folio::Site::Batch;

use strict;
use warnings;

sub new {
    my $cls = shift;
    my $self = bless { @_ }, $cls;
    $self->init;
    return $self;
}

sub loader { @_ > 1 ? $_[0]{'loader'} = $_[1] : $_[0]{'loader'} }
sub kind { @_ > 1 ? $_[0]{'kind'} = $_[1] : $_[0]{'kind'} }
sub members { @_ > 1 ? $_[0]{'members'} = $_[1] : $_[0]{'members'} }
sub is_prepared { @_ > 1 ? $_[0]{'is_prepared'} = $_[1] : $_[0]{'is_prepared'} }
sub is_loaded { @_ > 1 ? $_[0]{'is_loaded'} = $_[1] : $_[0]{'is_loaded'} }
sub results { @_ > 1 ? $_[0]{'results'} = $_[1] : $_[0]{'results'} }

sub init {
    my ($self) = @_;
    $self->{'members'} ||= [];
    # Always undefined until this point:
    $self->{'results'} = {};
}

sub add {
    my $self = shift;
    push @{ $self->members }, @_;
}

#sub load {
#    my ($self, %arg) = @_;
#    $self->loader->load('batch' => $self, %arg);
#    $self->members = [];
#}

1;
