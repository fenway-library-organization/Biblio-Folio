package Biblio::Folio::Object::Item;

sub location {
    my ($self) = @_;
    return $self->effective_location if $self->{'effectiveLocationId'};
    return $self->permanent_location;
}

1;
