package Biblio::Folio::Instance;

sub ttl { 1 }

sub _obj_uri { '/instance-storage/instances/%s' }

sub holdings {
    my ($self, $id_or_query) = @_;
    my $site = $self->site;
    my $holdings;
    if (!defined $id_or_query) {
        return @{ $self->{'holdings'} }
            if $self->{'holdings'};
        my $id = $self->{'id'};
        $holdings = $site->objects('/holdings-storage/holdings', 'query' => "instanceId==$id");
    }
    elsif (!ref $id_or_query) {
        $holdings = $site->objects('/holdings-storage/holdings', 'query' => $id_or_query);
    }
    else {
        $holdings = $site->objects('/holdings-storage/holdings', %$id_or_query);
    }
    return if !$holdings;
    $self->{'holdings'} = $holdings;
    return map { Biblio::Folio::HoldingsRecord->new('_site' => $site, 'instance' => $self, %$_) } @$holdings;
}

1;
