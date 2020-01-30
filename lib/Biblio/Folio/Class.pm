package Biblio::Folio::Class;

sub new {
    my $cls = shift;
    my $self = bless {
        'is_defined' => 0,
        @_,
    }, $cls;
    return $self->init;
}

sub site { return $_[0]{'site'} }
sub is_defined { return $_[0]{'is_defined'} }

sub init {
    my ($self) = @_;
}

sub define {
    my $self = shift;
    my $site = $self->site;

    foreach my $class (@_) {
        $class = $self->class($class) if !ref $class;
        my $pkg = $class->{'package'} or next;
        my $ttl = $class->{'ttl'} || 1;
        my $uri_fetch = _quote($class->{'uri'}{'fetch'} || die);
        my $uri_search = _quote($class->{'uri'}{'search'});
        my $pkg_code = qq{
            package $pkg;
            our \@${pkg}::ISA = qw(Biblio::Folio::Object);
            our \%${pkg}::uris = (
                'fetch' => $uri_fetch,
                'search' => $uri_search,
            );
            sub _ttl { $ttl }
            sub _uri { return \$${pkg}::uris{shift()} }
            eval { use $pkg };  # In case we have a separate module for it
        };
        my $ok = eval { eval $pkg_code; 1 };
        die $@ if !$ok;
        $class->{'defined'} = 1;
    }
}

1;
