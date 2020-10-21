package Biblio::Folio::Site::Profile;

use strict;
use warnings;

use base qw(Biblio::Folio::Object);

sub init {
    my ($self) = @_;
    $self->SUPER::init;
    my $profile = delete $self->{'profile'};  # Info about the profile itself
    if ($profile) {
        foreach (my ($k, $v) = each %$profile) {
            $self->{$k} = $v if !defined $self->{$k};
        }
    }
    return $self;
}

1;
