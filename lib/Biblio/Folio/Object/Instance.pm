package Biblio::Folio::Object::Instance;

# sub _obj_uri { '/instance-storage/instances/%s' }

sub holdings {
    my ($self, $id_or_query) = @_;
    my $site = $self->site;
    my @holdings;
    my $uri = $self->_uri_search;
    if (!defined $id_or_query) {
        return @{ $self->{'holdings'} } if $self->{'holdings'};
        my $id = $self->{'id'};
        @holdings = $site->object('holdings_record', 'query' => "instanceId==$id");
    }
    elsif (!ref $id_or_query) {
        @holdings = $site->object('holdings_record', 'query' => $id_or_query);
    }
    else {
        @holdings = $site->object('holdings_record', %$id_or_query);
    }
    $_->{'instance'} = $self for @holdings;
    $self->{'holdings'} = \@holdings;
    return @holdings;
}

sub from_marcref {
    my ($self, $marcref) = @_;
    my $marc = Biblio::Folio::Object::MARC->new('marcref' => $marcref)->parse;
    my $maker = Biblio::Folio::Site::MARC::InstanceMaker->new('site' => $self->site);
    return $maker->make($marcref);
}

1;
