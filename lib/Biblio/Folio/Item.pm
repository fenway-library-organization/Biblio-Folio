package Biblio::Folio::Item;

sub ttl { 1 }

sub _obj_uri { '/item-storage/items/%s' }

sub location {
    my ($self) = @_;
    return $self->cached('location' => $self->{'effectiveLocationId'});
}

1;
