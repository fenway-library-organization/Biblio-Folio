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
sub is_prepared { @_ > 1 ? $_[0]{'is_prepared'} = $_[1] : $_[0]{'is_prepared'} }
sub is_loaded { @_ > 1 ? $_[0]{'is_loaded'} = $_[1] : $_[0]{'is_loaded'} }
sub results { @_ > 1 ? $_[0]{'results'} = $_[1] : $_[0]{'results'} }

sub init {
    my ($self) = @_;
    $self->{'members'} ||= [];
    # Always undefined until this point:
    $self->{'results'} = {};
}

sub members {
    my $self = shift;
    if (@_ == 0) {
        return @{ $self->{'members'} } if wantarray;
        return $self->{'members'};
    }
    elsif (@_ > 1 || ref($_[0]) ne 'ARRAY') {
        $self->{'members'} = [ @_ ];
    }
    elsif (@_ == 1) {
        $self->{'members'} = shift;
    }
    else {
        die "bad call";
    }
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
