package Biblio::Folio::Site::Searcher;

use strict;
use warnings;

use Biblio::Folio::Util qw(_kind2pkg _cql_term _cql_and);

sub new {
    my $cls = shift;
    my $self = bless { @_ }, $cls;
    $self->init;
    return $self;
}

sub init {
    my ($self) = @_;
    $self->{'buffer'} = [];
    $self->{'finished'} = $self->{'prepared'} = 0;
}

sub site { @_ > 1 ? $_[0]{'site'} = $_[1] : $_[0]{'site'} }
sub kind { @_ > 1 ? $_[0]{'kind'} = $_[1] : $_[0]{'kind'} }
sub terms { @_ > 1 ? $_[0]{'terms'} = $_[1] : $_[0]{'terms'} }

sub params { @_ > 1 ? $_[0]{'params'} = $_[1] : $_[0]{'params'} }
sub offset { @_ > 1 ? $_[0]{'params'}{'offset'} = $_[1] : $_[0]{'params'}{'offset'} }
sub limit { @_ > 1 ? $_[0]{'params'}{'limit'} = $_[1] : $_[0]{'params'}{'limit'} }
sub query { @_ > 1 ? $_[0]{'params'}{'query'} = $_[1] : $_[0]{'params'}{'query'} }
sub uri { @_ > 1 ? $_[0]{'params'}{'uri'} = $_[1] : $_[0]{'params'}{'uri'} }

sub buffer { @_ > 1 ? $_[0]{'buffer'} = $_[1] : $_[0]{'buffer'} }
sub prepared { @_ > 1 ? $_[0]{'prepared'} = $_[1] : $_[0]{'prepared'} }
sub finished { @_ > 1 ? $_[0]{'finished'} = $_[1] : $_[0]{'finished'} }

# while (!$search->finished) {
#     my $obj = $search->next or last;
#     ...
# }
sub next {
    my ($self) = @_;
    return if $self->finished;
    $self->prepare if !$self->prepared;
    my $buffer = $self->buffer;
    if (!@$buffer) {
        $self->search or return;
    }
    return shift @$buffer;
}

sub all {
    my ($self) = @_;
    return if $self->finished;
    $self->prepare if !$self->prepared;
    my $site = $self->site;
    my $kind = $self->kind;
    my %param = %{ $self->params };
    my @all = @{ $self->buffer };  # Probably empty, but you never know
    while (my @objects = $site->object($kind, %param)) {
        push @all, @objects;
        $param{'offset'} += @objects;
    }
    $self->finished(1);
    $self->buffer([]);
    return @all;
}

sub prepare {
    my ($self, $offset, $limit) = @_;
    my $kind = $self->kind;
    my $terms = $self->terms;
    my $query = $self->query;
    my (%param, @buffer);
    $self->{'buffer'} = \@buffer;
    $self->{'params'} = \%param;
    $self->{'prepared'} = $self->{'finished'} = 0;
    $param{'offset'} = $offset || $self->offset || 0;
    $param{'limit'} = $limit || $self->limit || 100;
    if (defined $query) {
        $param{'query'} = $query;
    }
    elsif ($terms) {
        my @terms;
        my $exact = {'exact' => 1};
        while (my ($k, $v) = each %$terms) {
            if (ref($v) eq 'ARRAY') {
                push @terms, _cql_term($k, $v, $exact, $k =~ /id$/i);
            }
            else {
                push @terms, _cql_term($k, $v, $exact);
            }
        }
        $param{'query'} = _cql_and(@terms);
    }
    else {
        my $pkg = _kind2pkg($kind);
        $param{'uri'} = $pkg->_uri_search || $pkg->_uri
            or die "can't determine URI for searching kind $kind";
    }
    $self->{'prepared'} = 1;
    return $self;
}

sub search {
    my ($self) = @_;
    my $params = $self->params;
    my $buffer = $self->buffer;
    my @objects = $self->site->object($self->kind, %$params);
    if (@objects) {
        push @$buffer, @objects;
        $params->{'offset'} += @objects;
        return shift @$buffer;
    }
    else {
        $self->finished(1);
        return;
    }
}

1;
