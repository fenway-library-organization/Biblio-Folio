package Biblio::Folio::Site::BatchLoader;

# my $loader = $cls->new('profile' => $profile, ...);
# my $matcher = $cls->new('profile' => $profile, ...);
# my $batch_file = $cls->new(...);
# my $batch = $loader->prepare('match_results' => \@match_results);
# my @results = $loader->load($batch);

use strict;
use warnings;

use Biblio::Folio::Site::Batch;

my %action2make = (
    'create' => sub {
        my ($item) = @_;
        @$item{qw(method uri)} = ('POST', $item->object->_uri_create);
    },
    'update' => sub {
        my ($item) = @_;
        @$item{qw(method uri)} = ('PUT', $item->object->_uri_update);
    },
    'delete' => sub {
        my ($item) = @_;
        @$item{qw(method uri)} = ('PUT', $item->object->_uri_delete);
    },
);

sub new {
    my $cls = shift;
    my $self = bless { @_ }, $cls;
    $self->init;
    return $self;
}

sub site { @_ > 1 ? $_[0]{'site'} = $_[1] : $_[0]{'site'} }
sub profile { @_ > 1 ? $_[0]{'profile'} = $_[1] : $_[0]{'profile'} }
sub kind { @_ > 1 ? $_[0]{'kind'} = $_[1] : $_[0]{'kind'} }
sub items { @_ > 1 ? $_[0]{'items'} = $_[1] : $_[0]{'items'} }

sub init {
    my ($self) = @_;
    $self->{'items'} = [];
}

sub prepare {
    my $self = shift;
    my @batch;
    my $batch = Biblio::Folio::Site::Batch->new(
        'loader' => $self,
        'kind' => $self->kind,
    );
    my $prep_all = $self->can('prepare_all');
    if ($prep_all) {
        my @items = $prep_all->($self, @_);
        $batch->add(@items);
    }
    else {
        my $prep_one = $self->can('prepare_one')
            or die "can't prepare one at a time or all at once";
        foreach my $one (@_) {
            my $item = $prep_one->($self, $one);
            $batch->add($item);
        }
    }
    $batch->is_prepared(1);
    return $batch;
}

sub _make_create {
    my ($self, $item) = @_;
    $item->{'method'} = 'POST';
    $item->{'uri'} = $item->{'object'}->_uri_create;
    return $item;
}

sub _make_update {
    my ($self, $item) = @_;
    $item->{'method'} = 'PUT';
    $item->{'uri'} = $item->{'object'}->_uri_update;
    return $item;
}

sub _make_delete {
    my ($self, $item) = @_;
    $item->{'method'} = 'DELETE';
    $item->{'uri'} = $item->{'object'}->_uri_delete;
    return $item;
}

sub prepare_one {
    # Default implementation
    my ($self, $item) = @_;
    my $profile = $self->profile;
    my $match_action = $self->action('match');
    my $matches_action = $self->action('matches');
    my $nomatch_action = $self->action('nomatch');
    my @matches = @{ $item->{'matches'} };
    if (@matches == 0) {
        $item->{'method'} = 'POST';
    }
    1;  # TODO Update $item->{'object'}
    return $item;  # TODO
}

sub _make_requests {
    # $loader->_make_requests($batch);
    my ($self, $batch, %arg) = @_;
    my $method = $batch->{'method'} || $arg{'method'};
    my $uri = $batch->{'uri'} || $arg{'uri'};
    if ($method xor $uri) {
        my $msg = $method ? "method $method but no URI"
                          : "URI $uri but no method";
        my $file = $batch->file;
        my $kind = $batch->kind;
        die "batch $file ($kind file) can't be loaded: it has $msg";
    }
    my $items = $batch->items;
    my $site = $self->site;
    my @requests;
    foreach my $item (@$items) {
        my $a = $item->{'action'} or next;
        if (defined $method) {
            @$item{qw(method uri)} = ($method, $uri);
        }
        else {
            my $make = $action2make{$a} or next;  # nothing to do
            $make->($item);
        }
        my ($m, $u, $o) = @$item{qw(method uri object)};
        $item->{'request'} = $site->make_request($m, $u, $o);
    }
    return $self;
}

sub load {
    # my ($num_ok, $num_failed) = $loader->load($batch);
    # my $ok = $loader->load($batch);
    my ($self, $batch, %arg) = @_;
    $self->_make_requests($batch, %arg);
    my $site = $self->site;
    my $n = 0;
    my (@ok, @failed);
    my $items = $batch->items;
    foreach my $item (@$items) {
        $n++;
        my $req = $item->{'request'};
        my $res = $site->req($req);
        my %result = (
            'n' => $n,
            'item' => $item,
        );
        if (!defined $res) {
            push @failed, $item;
            $item->{'status'} = '503 Service Unavailable',
            $item->{'error'} = 'no response received',
        }
        elsif ($res->is_success) {
            push @ok, $item;
            $item->{'ok'} = 1;
        }
        else {
            push @failed, $item;
            $item->{'status'} = $res->status;
            $item->{'error'} = $res->content;
        }
    }
    my %result;
    $result{'ok'} = \@ok if @ok;
    $result{'failed'} = \@failed if @failed;
    $batch->results(\%result);
    $batch->is_loaded(1);
    return (scalar(@ok), scalar(@failed)) if wantarray;
    return @failed ? 0 : 1;
}

1;
