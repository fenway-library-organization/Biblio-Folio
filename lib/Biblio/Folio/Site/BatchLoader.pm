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
        my ($member) = @_;
        @$member{qw(method uri)} = ('POST', $member->object->_uri_create);
    },
    'update' => sub {
        my ($member) = @_;
        @$member{qw(method uri)} = ('PUT', $member->object->_uri_update);
    },
    'delete' => sub {
        my ($member) = @_;
        @$member{qw(method uri)} = ('PUT', $member->object->_uri_delete);
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
sub members { @_ > 1 ? $_[0]{'members'} = $_[1] : $_[0]{'members'} }

sub init {
    my ($self) = @_;
    $self->{'members'} = [];
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

### sub prepare_one {
###     # Default implementation
###     my ($self, $member) = @_;
###     my $profile = $self->profile;
###     my $match_action = $self->action('match');
###     my $matches_action = $self->action('matches');
###     my $nomatch_action = $self->action('nomatch');
###     my @matches = @{ $member->{'matches'} };
###     if (@matches == 0) {
###         $member->{'method'} = 'POST';
###     }
###     1;  # TODO Update $member->{'object'}
###     return $member;  # TODO
### }

sub prepare_one {
    my ($self, $member) = @_;
    my ($record, $matches) = @$member{qw(record matches)};
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
            my $rv = $robject->{$field};
            next if !defined $rv;  # TODO What if $rv is an array ref?
            my $weight = $tf->{'tiebreaker'};
            foreach my $match (@matches) {
                my $n = $match->{'n'};
                my $mv = $match->{'object'}{$field};
                next if !defined $mv || $mv ne $rv;  # TODO What if $mv is an array ref?
                my $score = $match->{'score'} += $weight;
                $max_score = $score if $score > $max_score;
            }
        }
        @matches = sort { $b->{'score'} <=> $a->{'score'} } @matches;
    }
    my @winners = map { $_->{'score'} == $max_score ? ($_) : () } @matches;
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
### my $rpg = $record->{'patronGroup'};
### my @pg_matches = grep { $_->{'object'}{'patronGroup'} eq $rpg } @$matches;
### if (@$matches == 0) {
###     # No matches at all
###     $action = $actions->{'noMatch'} || 'create';
### }
### elsif (@pg_matches == 1) {
###     # One match with the right patronGroup
###     $action = $actions->{'oneMatch'} || 'update';
###     $one_match = $pg_matches[0]{'object'};
### }
### elsif (@pg_matches > 1) {
###     # Too many matches
###     $action = $actions->{'multipleMatches'} || 'skip';
###     $member->{'warning'} = "too many matches:";
### }
### elsif (@$matches == 1) {
###     # One match, even though the patronGroup is different
###     $action = $actions->{'oneMatch'} || 'update';
###     $one_match = $matches->[0]{'object'};
### }
    $member->{'action'} = $action;
    my $sub = $self->can('_prepare_'.$action);
    $sub->($self, $member) if $sub;
    return $member;
}

sub _prepare_create {
    my ($self, $member) = @_;
    $member->{'object'}{'id'} = $self->site->folio->uuid;
}

sub _prepare_delete { }

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
    my $members = $batch->members;
    my $site = $self->site;
    my @requests;
    foreach my $member (@$members) {
        my $a = $member->{'action'} or next;
        if (defined $method) {
            @$member{qw(method uri)} = ($method, $uri);
        }
        else {
            my $make = $action2make{$a} or next;  # nothing to do
            $make->($member);
        }
        my ($m, $u, $o) = @$member{qw(method uri object)};
        $member->{'request'} = $site->make_request($m, $u, $o);
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
    foreach my $member (@$members) {
        $n++;
        my $req = $member->{'request'};
        my $res = $site->req($req);
        my %result = (
            'n' => $n,
            'member' => $member,
        );
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
    my %result;
    $result{'ok'} = \@ok if @ok;
    $result{'failed'} = \@failed if @failed;
    $batch->results(\%result);
    $batch->is_loaded(1);
    return (scalar(@ok), scalar(@failed)) if wantarray;
    return @failed ? 0 : 1;
}

1;
