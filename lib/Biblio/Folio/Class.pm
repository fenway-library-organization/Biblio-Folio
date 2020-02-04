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

sub uri { return $_[0]{'uri'}{$_[1]} }

sub init {
    my ($self) = @_;
    return $self;
}

1;
