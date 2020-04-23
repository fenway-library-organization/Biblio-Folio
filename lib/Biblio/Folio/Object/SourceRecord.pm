package Biblio::Folio::Object::SourceRecord;

sub ttl { 1 }

sub as_marc {
    my ($self) = @_;
    return $self->{'rawRecord'}{'content'};
}

1;
