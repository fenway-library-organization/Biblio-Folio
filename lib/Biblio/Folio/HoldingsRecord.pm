package Biblio::Folio::HoldingsRecord;

sub ttl { 1 }

sub _obj_uri { '/holdings-storage/holdings/%s' }

sub call_number { shift()->{'callNumber'} }

sub items {
    my ($self, $id_or_query) = @_;
    my $site = $self->site;
    my $items;
    if (!defined $id_or_query) {
        return @{ $self->{'items'} }
            if $self->{'items'};
        my $id = $self->{'id'};
        $items = $site->objects('/item-storage/items', 'query' => "holdingsRecordId==$id");
    }
    elsif (!ref $id_or_query) {
        $items = $site->objects('/item-storage/items', 'query' => $id_or_query);
    }
    else {
        $items = $site->objects('/item-storage/items', %$id_or_query);
    }
    return if !$items;
    $self->{'items'} = $items;
    return map { Biblio::Folio::Item->new('_site' => $site, 'holdingsRecord' => $self, %$_) } @$items;
}

sub permanent_location {
    my ($self) = @_;
    return $self->{'permanentLocation'} = $self->cached('location' => $self->{'permanentLocationId'});
}

sub effective_location {
    my ($self) = @_;
    return $self->{'effectiveLocation'} = $self->cached('location' => $self->{'effectiveLocationId'});
}

sub call_number_type {
    my ($self) = @_;
    return $self->{'callNumberType'} = $self->cached('call_number_type' => $self->{'callNumberTypeId'});
}

1;
