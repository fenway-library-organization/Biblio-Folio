package Biblio::Folio::Site::BatchLoader;

use strict;
use warnings;

@Biblio::Folio::Site::BatchLoader::ISA = qw(Biblio::Folio::Object);

sub profile { @_ > 1 ? $_[0]{'profile'} = $_[1] : $_[0]{'profile'} }
sub queue { @_ > 1 ? $_[0]{'queue'} = $_[1] : $_[0]{'queue'} }

sub init {
    my ($self) = @_;
    $self->{'queue'} = [];
}

sub enqueue {
    # $loader->enqueue($method, @objects);
    # $loader->enqueue($method, $uri, @objects);
    die "bad call" if @_ < 3;
    my $self = shift;
    my $method = shift;
    my $what = $_[0];
    my $r = ref $what;
    my $uri;
    if ($r eq '' || $what->isa('URI')) {
        $uri = shift;
    }
    my $queue = $self->queue;
    my $site = $self->site;
    foreach my $obj (@_) {
        $r = ref $obj;
        my $u = $uri;
        if (!defined $u) {
            my $urikey = '_uri_' . lc $method;
            $u = eval { $obj->$urikey }
                or die "don't know how to $method a(n) $r";
        }
        my @params;
        while ($u =~ s/{([^{}]+)}$/%s/) {
            my $k = $1;
            my $v = $obj->{$k};
            die "no $k to use to $method a(n) $r" if !defined $v;
            push @params, $v;
        }
        $u = sprintf $u, @params if @params;
        push @$queue, $site->make_request($method, $u, $obj);
    }
    return $self;
}

sub load {
    my $self = shift;
    $self->enqueue(@_) if @_;
    XXX;
}

1;
