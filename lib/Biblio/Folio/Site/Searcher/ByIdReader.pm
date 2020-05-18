package Biblio::Folio::Site::Searcher::ByIdReader;

use strict;
use warnings;

use Biblio::Folio::Site::Searcher;

use vars qw(@ISA);

@ISA = qw(Biblio::Folio::Site::Searcher);

sub new {
    my $cls = shift;
    unshift @_, 'file' if @_ % 2;
    return $cls->SUPER::new(@_);
}

sub init {
    my ($self) = @_;
    $self->SUPER::init;
    $self->{'not_found'} = {};
    $self->{'seen'} = {} if $self->{'dedup'};
    $self->{'id_field'} ||= 'id';
    $self->{'batch_size'} ||= 25;   # How many IDs to search for at once
    $self->{'limit'} ||= 100;       # How many objects to fetch at once
}

sub reader { @_ > 1 ? $_[0]{'reader'} = $_[1] : $_[0]{'reader'} }
sub searcher { @_ > 1 ? $_[0]{'searcher'} = $_[1] : $_[0]{'searcher'} }
sub dedup { @_ > 1 ? $_[0]{'dedup'} = $_[1] : $_[0]{'dedup'} }
sub seen { @_ > 1 ? $_[0]{'seen'} = $_[1] : $_[0]{'seen'} }
sub id_field { @_ > 1 ? $_[0]{'id_field'} = $_[1] : $_[0]{'id_field'} }
sub not_found { @_ > 1 ? $_[0]{'not_found'} = $_[1] : $_[0]{'not_found'} }
sub batch_size { @_ > 1 ? $_[0]{'batch_size'} = $_[1] : $_[0]{'batch_size'} }

sub uri {
    my $self = shift;
    my $uri = $self->SUPER::uri(@_);
    my $searcher = $self->searcher or return $uri;
    $searcher->uri(@_);
}

sub offset {
    my $self = shift;
    my $offset = $self->SUPER::offset(@_);
    my $searcher = $self->searcher or return $offset;
    $searcher->offset(@_);
}

sub limit {
    my $self = shift;
    my $limit = $self->SUPER::limit(@_);
    my $searcher = $self->searcher or return $limit;
    $searcher->limit(@_);
}

sub next {
    my ($self, $n) = @_;
    $n ||= wantarray ? $self->batch_size : 1;
    my @objects;
    my $searcher = $self->searcher;
    my $not_found = $self->not_found;
    while (@objects < $n) {
        $searcher = $self->next_searcher if !$searcher || $searcher->finished;
        if (wantarray) {
            return @objects if !$searcher;
            my @more_objects = $searcher->next($n - @objects);
            undef($searcher), next if !@more_objects;
            delete $not_found->{$_->id} for @more_objects;
            push @objects, @more_objects;
        }
        elsif (!$searcher) {
            return if !@objects;
            return shift @objects;
        }
        else {
            my $obj = $searcher->next;
            undef $searcher;
            if ($obj) {
                delete $not_found->{$obj->id};
                return $obj;
            }
        }
    }
    return if !@objects;
    return wantarray ? @objects : $objects[0];
}

sub all {
    my ($self) = @_;
    my $n = $self->limit;
    my @all;
    while (my @objects = $self->next($n)) {
        push @all, @objects;
    }
    return @all;
}

sub read_more {
    my ($self) = @_;
    my $reader = $self->reader;
    my $batch_size = $self->batch_size;
    my $not_found = $self->not_found;
    my $dedup = $self->dedup;
    my $seen = $self->seen;
    my @ids;
    while ($batch_size) {
        my $id = $self->read_one;
        last if !defined $id;
        next if $dedup && $seen->{$id}++;
        $not_found->{$id} = 1;
        push @ids, $id;
        $batch_size--;
    }
    return @ids;
}

sub next_searcher {
    my ($self) = @_;
    my @ids = $self->read_more;
    if (!@ids) {
        $self->finished(1);
        return;
    }
    my $site = $self->site;
    my $id_field = $self->id_field;
    my $searcher = $site->searcher($self->kind, $id_field => [@ids], '@limit' => $self->limit);
    $self->searcher($searcher);
    return $searcher;
}

1;
