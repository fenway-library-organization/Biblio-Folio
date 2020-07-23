package Biblio::Folio::Site::BatchLoader;

# my $loader = $cls->new('profile' => $profile, ...);
# my $matcher = $cls->new('profile' => $profile, ...);
# my $batch_file = $cls->new(...);
# my $batch = $loader->prepare('match_results' => \@match_results);
# my @results = $loader->load($batch);

use strict;
use warnings;

use Biblio::Folio::Site::Batch;
use Biblio::Folio::Util qw(_uuid _cmpable _unbless _kind2pkg);
use Scalar::Util qw(blessed);

sub new {
    my $cls = shift;
    my $self = bless { @_ }, $cls;
    $self->init;
    return $self;
}

sub site { @_ > 1 ? $_[0]{'site'} = $_[1] : $_[0]{'site'} }
sub profile { @_ > 1 ? $_[0]{'profile'} = $_[1] : $_[0]{'profile'} }
sub kind { @_ > 1 ? $_[0]{'kind'} = $_[1] : $_[0]{'kind'} }
sub class { @_ > 1 ? $_[0]{'class'} = $_[1] : $_[0]{'class'} }
sub actions { @_ > 1 ? $_[0]{'actions'} = $_[1] : $_[0]{'actions'} }
sub members { @_ > 1 ? $_[0]{'members'} = $_[1] : $_[0]{'members'} }

sub init {
    my ($self) = @_;
    $self->{'members'} = [];
    my $kind = $self->{'kind'};
    my $cls = $self->{'class'} = _kind2pkg($kind);
    my %can_cache;
    $self->{'actions'} = {
        'create' => sub {
            my ($cls, $obj) = @_;
            my $sub = $can_cache{$cls.'::_uri_create'} ||= $cls->can('_uri_create');
            return { 'method' => 'POST', 'uri' => $sub->($cls, $obj) };
        },
        'update' => sub {
            my ($cls, $obj) = @_;
            my $sub = $can_cache{$cls.'::_uri_update'} ||= $cls->can('_uri_update');
            return { 'method' => 'PUT', 'uri' => $sub->($cls, $obj) };
        },
        'delete' => sub {
            my ($cls, $obj) = @_;
            my $sub = $can_cache{$cls.'::_uri_delete'} ||= $cls->can('_uri_delete');
            return { 'method' => 'DELETE', 'uri' => $sub->($cls, $obj) };
        },
        'skip' => undef,
        'none' => undef,  # XXX
    };
}

sub prepare {
    my $self = shift;
    my @batch;
    my $batch = Biblio::Folio::Site::Batch->new(
        'loader' => $self,
        'kind' => $self->kind,
        'class' => $self->class,
    );
    my $prep_all = $self->can('prepare_all');
    if ($prep_all) {
        my @members = $prep_all->($self, @_);
        $batch->add(@members);
    }
    else {
        my $prep_one = $self->can('prepare_one')
            or die "can't prepare one at a time or all at once";
        foreach my $one (@_) {
            my $member = $prep_one->($self, $one);
            $batch->add($member);
        }
    }
    $batch->is_prepared(1);
    return $batch;
}

sub _make_create {
    my ($self, $member) = @_;
    $member->{'method'} = 'POST';
    $member->{'uri'} = $member->{'object'}->_uri_create;
    return $member;
}

sub _make_update {
    my ($self, $member) = @_;
    $member->{'method'} = 'PUT';
    $member->{'uri'} = $member->{'object'}->_uri_update;
    return $member;
}

sub _make_delete {
    my ($self, $member) = @_;
    $member->{'method'} = 'DELETE';
    $member->{'uri'} = $member->{'object'}->_uri_delete;
    return $member;
}

sub prepare_one {
    my ($self, $member) = @_;
    my $record = $member->{'record'};
    my $matches = $member->{'matches'} ||= [];
    my $profile = $self->profile;
    my $actions = $profile->{'actions'};
    # See if there is exactly one suitable match
    my ($one_match, $action);
    my @matches = @$matches;
    my $max_score = 0;
    if (@matches > 1) {
        my @tiebreakers = $self->tiebreakers;
        $_->{'score'} = 0 for @matches;
        my $robject = $record->{'object'};
        foreach my $tf (@tiebreakers) {
            my $field = $tf->{'field'};
            my $rv = _cmpable($robject->{$field});
            next if !defined $rv;
            my $weight = $tf->{'tiebreaker'};
            foreach my $match (@matches) {
                my $n = $match->{'n'};
                my $mv = _cmpable($match->{'object'}{$field});
                next if !defined $mv || $mv ne $rv;
                my $score = $match->{'score'} += $weight;
                $max_score = $score if $score > $max_score;
            }
        }
        @matches = sort { $b->{'score'} <=> $a->{'score'} } @matches;
    }
    my @winners = map { $_->{'score'} || 0 == $max_score ? ($_) : () } @matches;
    if (@winners == 0) {
        # No matches at all
        $action = $actions->{'noMatch'} || 'create';
    }
    elsif (@winners == 1) {
        $action = $actions->{'oneMatch'} || 'update';
        $member->{'object'} = $winners[0]{'object'};
    }
    elsif (@winners > 1) {
        # Too many matches, and we couldn't break the tie
        $action = $actions->{'multipleMatches'} || 'skip';
        $member->{'warning'} = "too many matches:";
    }
    $member->{'action'} = $action;
    my $sub = $self->can('_prepare_'.$action);
    $sub->($self, $member) if $sub;
    return $member;
}

sub _prepare_create {
    my ($self, $member) = @_;
# _uuid($member->{'object'});  # Assign a UUID if it doesn't already have one
}

sub _prepare_delete {
    my ($self, $member);
    # XXX Nothing to do?
}


sub _prepare_update {
    my ($self, $member);
    # XXX Nothing to do?
}

sub _make_requests {
    # $loader->_make_requests($batch);
    my ($self, $batch, %arg) = @_;
    my $kind = $batch->kind;
    my $cls = _kind2pkg($kind);
    my $method = $batch->{'method'} || $arg{'method'};
    my $uri = $batch->{'uri'} || $arg{'uri'};
    if ($method xor $uri) {
        my $msg = $method ? "method $method but no URI"
                          : "URI $uri but no method";
        my $file = $batch->file;
        die "batch $file ($kind file) can't be loaded: it has $msg";
    }
    my $members = $batch->members;
    my $site = $self->site;
    my @requests;
    my $actions = $self->actions;
    foreach my $member (@$members) {
        my $a = $member->{'action'} or next;  # XXX
        my $obj = $member->{$a} or next;  # XXX
        my ($m, $u);
        if (defined $method) {
            ($m, $u) = @$member{qw(method uri)} = ($method, $uri);
        }
        else {
            my $sub = $actions->{$a} or next;  # XXX
            my $action = $sub->($cls, $obj) or next;
            ($m, $u) = @$member{qw(method uri)} = @$action{qw(method uri)};
        }
        $member->{'_request'} = $site->make_request($m, $u, $obj);
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
    my $members = $batch->members;
    if ($batch->{'request'}) {
        # Batch load
        my $req = $batch->{'request'};
        my $res = $site->req($req);
        if (!defined $res) {
            @failed = @$members;
        }
        elsif ($res->is_success) {
            @ok = @$members;
            $_->{'ok'} = 1 for @ok;
        }
        else {
            # XXX
            @failed = @$members;
            my ($status, $content) = ($res->status, $res->content);
            @$_{qw(status error)} = ($status, $content) for @$members;
        }
    }
    else {
        foreach my $member (@$members) {
            $n++;
            my $req = $member->{'_request'}
                or next;  # Skip or unchanged
            my $res = $site->req($req);
            if (!defined $res) {
                push @failed, $member;
                $member->{'status'} = '503 Service Unavailable',
                $member->{'error'} = 'no response received',
            }
            elsif ($res->is_success) {
                push @ok, $member;
                $member->{'ok'} = 1;
            }
            else {
                push @failed, $member;
                $member->{'status'} = $res->status;
                $member->{'error'} = $res->content;
            }
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
