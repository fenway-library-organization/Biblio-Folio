package Biblio::Folio::HoldingsRecord;

# sub call_number { shift()->{'callNumber'} }

sub items {
    my ($self, $id_or_query) = @_;
    my $site = $self->site;
    my @items;
    if (!defined $id_or_query) {
        return @{ $self->{'items'} } if $self->{'items'};
        my $id = $self->{'id'};
        @items = $site->object('item', 'query' => "holdingsRecordId==$id");
    }
    elsif (!ref $id_or_query) {
        @items = $site->objects('item', 'query' => $id_or_query);
    }
    else {
        @items = $site->objects('item', %$id_or_query);
    }
    $_->{'holdings_record'} = $self for @items;
    $self->{'items'} = \@items;
    return @items;
}

sub location {
    my ($self) = @_;
    return $self->effective_location if $self->{'effectiveLocationId'};
    return $self->permanent_location;
}

#sub permanent_location {
#    my ($self) = @_;
#    return $self->{'permanentLocation'} = $self->cached('location' => $self->{'permanentLocationId'});
#}
#
#sub effective_location {
#    my ($self) = @_;
#    return $self->{'effectiveLocation'} = $self->cached('location' => $self->{'effectiveLocationId'});
#}
#
#sub call_number_type {
#    my ($self) = @_;
#    return $self->{'callNumberType'} = $self->cached('call_number_type' => $self->{'callNumberTypeId'});
#}

1;
