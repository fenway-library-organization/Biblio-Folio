package Biblio::Folio::Site::Searcher;

use strict;
use warnings;

use Biblio::Folio::Util qw(_kind2pkg _cql_query _use_class);

# my $searcher = Biblio::Folio::Site::Searcher->new(
#     'kind' => $kind,    # E.g., 'user' or 'source_record'
#     'uri' => $uri,      # Optional (and rarely if ever needed)
#     'query' => $cql,    # \ Only one of these two
#     'terms' => \%term,  # / should be specified
# );
# while (!$searcher->finished) {
#     my $obj = $searcher->next or last;
#     my @obj = $searcher->next($n) or last;
#     ...
# }
# -or-
# while (my $obj = $searcher-next) {
#     ...
# }
# -or- (maybe, later)
# $searcher->iterate(1, sub {
#     my ($obj) = @_;
# });

my %have_used;

sub new {
    my $cls = shift;
    my $self = bless { @_ }, $cls;
    $self->init;
    return $self;
}

sub by {
    my ($self, %arg) = @_;
    my $pkg;
    if ($arg{'file'}) {
        $pkg = 'Biblio::Folio::Site::Searcher::ByIdFile';
    }
    elsif ($arg{'set'}) {
        $pkg = 'Biblio::Folio::Site::Searcher::ByIdSet';
    }
    else {
        die;
    }
    _use_class($pkg) if !$have_used{$pkg}++;
    return $pkg->new(
        %$self,
        %arg,
    );
}

sub init {
    my ($self) = @_;
    $self->{'buffer'} = [];
    $self->{'finished'} = $self->{'prepared'} = 0;
    $self->{'params'} ||= {
        'offset' => 0,
        'limit' => 10,
    };
}

sub site { @_ > 1 ? $_[0]{'site'} = $_[1] : $_[0]{'site'} }
sub kind { @_ > 1 ? $_[0]{'kind'} = $_[1] : $_[0]{'kind'} }
sub terms { @_ > 1 ? $_[0]{'terms'} = $_[1] : $_[0]{'terms'} }
sub buffer { @_ > 1 ? $_[0]{'buffer'} = $_[1] : $_[0]{'buffer'} }
sub prepared { @_ > 1 ? $_[0]{'prepared'} = $_[1] : $_[0]{'prepared'} }
sub finished { @_ > 1 ? $_[0]{'finished'} = $_[1] : $_[0]{'finished'} }

sub param {
    my $self = shift;
    my $key = shift;
    my $params = $self->{'params'};
    return $params->{$key} if !@_;
    my $val = shift;
    if (defined $val) {
        $params->{$key} = $val;
    }
    else {
        delete $params->{$key};
    }
    return $self;
}

sub params {
    my $self = shift;
    return $self->{'params'} if !@_;
    $self->{'params'} = shift;
    return $self;
}

sub offset {
    my $self = shift;
    return $self->{'params'}{'offset'} if !@_;
    $self->{'params'}{'offset'} = shift;
    return $self;
}

sub limit {
    my $self = shift;
    return $self->{'params'}{'limit'} if !@_;
    $self->{'params'}{'limit'} = shift;
    return $self;
}

sub query {
    my $self = shift;
    return $self->{'params'}{'query'} if !@_;
    $self->{'params'}{'query'} = shift;
    return $self;
}

sub uri {
    my $self = shift;
    return $self->{'params'}{'uri'} if !@_;
    $self->{'params'}{'uri'} = shift;
    return $self;
}

sub next {
    my ($self, $n) = @_;
    return if $self->finished;
    $self->prepare if !$self->prepared;
    my $buffer = $self->buffer;
    if (wantarray) {
        $n ||= $self->limit;
        while (@$buffer < $n) {
            $self->search or last;
        }
        $n = @$buffer if $n > @$buffer;
        return splice @$buffer, 0, $n;
    }
    elsif (!@$buffer) {
        $self->search or return;
    }
    warn __PACKAGE__ . q{::next misused: $obj = $searcher->next($n);}
        if $n && $n > 1;
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
    my $params = $self->params;
    $self->{'buffer'} = [];
    $self->{'prepared'} = $self->{'finished'} = 0;
    $params->{'offset'} = $offset if defined $offset;
    $params->{'limit'} = $limit if defined $limit;
    if (defined $query) {
        $params->{'query'} = $query;
    }
    elsif ($terms) {
        $params->{'query'} = _cql_query($terms);
    }
    else {
        my $pkg = _kind2pkg($kind);
        $params->{'uri'} ||= $pkg->_uri_search || $pkg->_uri
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
        return scalar @$buffer;
    }
    else {
        $self->finished(1);
        return;
    }
}

1;
