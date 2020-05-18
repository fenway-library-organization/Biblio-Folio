package Biblio::Folio::Site::Harvester;

use strict;
use warnings;

sub new {
    my $cls = shift;
    my $self = bless { @_ }, $cls;
    $self->init;
    return $self;
}

sub init {
    my ($self) = @_;
}

sub site { @_ > 1 ? $_[0]{'site'} = $_[1] : $_[0]{'site'} }

sub harvest {
    my ($self, %arg) = @_;
    %arg = (%$self, %arg);
    return $self->harvest_all(%arg) if $arg{'all'};
    my $site = $self->site;
    my $batch_size = $arg{'batch_size'} ||= 25;
    my ($query, $since) = @arg{qw(query since)};
    $self->set_caching_policy($arg{'cache'});
}

sub gather {
    my ($self, $searcher) = @_;
    my $kind = $searcher->kind;
    my $size = $searcher->batch_size;

    my $cache = $self->cache;

    my (%up, %down);

    my ($up_ckey, $up_pkind) = @{ $up{$kind} };
    my ($down_pkey, $down_ckind, $down_ckey) = @{ $down{$kind} };

    my $queue      = $self->queue($kind);
    my $up_queue   = $self->queue($up_pkind);
    my $down_queue = $self->queue($down_ckind);

    my $seen       = $self->seen($kind);
    my $up_seen    = $self->seen($up_pkind);
    my $down_seen  = $self->seen($down_ckind);

    while (my @objects = $searcher->next($size)) {
        foreach my $object (@objects) {
            my $id = $object->id;
            next if $seen->{$id}++;
            if ($up_queue) {
                my $up_id = $object->{$up_ckey};
                push @$up_queue, $up_id if defined $up_id;
            }
            if ($down_queue) {
                my $down_id = $object->{$down_pkey};
                push @$down_queue, $down_id if defined $down_id;
            }
            $cache->{$id} = $object;
        }
    }
    # $searcher is now finished
}

sub set_caching_policy {
    my ($self, $cp) = @_;
    my %dont_cache = map { $_ => 1 } qw(instance source_record holdings_record);
    if (defined $cp) {
        my $r = ref $cp;
        if ($r eq '') {
            delete $dont_cache{$cp};
        }
        elsif ($r eq 'HASH') {
            delete $dont_cache{$_} for keys %$cp;
        }
        elsif ($r eq 'ARRAY') {
            delete $dont_cache{$_} for @$cp;
        }
        else {
            die "internal error: caching policy must be scalar, hash, or array";
        }
    }
    my $site = $self->site;
    $site->dont_cache(keys %dont_cache);
}

1;
