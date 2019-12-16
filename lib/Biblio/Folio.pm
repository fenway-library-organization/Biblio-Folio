package Biblio::Folio;

use strict;
use warnings;

sub new {
    my $cls = shift;
    my $self = bless {
        'root' => '/usr/local/flolio',
        @_,
    }, $cls;
    $self->init;
    return $self;
}

sub root { @_ > 1 ? $_[0]{'root'} = $_[1] : $_[0]{'root'} }

sub site {
    my ($self, $name) = @_;
    return Biblio::Folio::Site->new($name, 'folio' => $self);
}

sub init {
    my ($self) = @_;
    my $root = $self->root;
}

# ------------------------------------------------------------------------------

package Biblio::Folio::Site;

use JSON;
use Digest;

my $json = JSON->new->pretty;

sub new {
    my $cls = shift;
    unshift @_, 'name' if @_ % 2;
    my $self = bless { @_ }, $cls;
    $self->init;
    return $self;
}

sub name { @_ > 1 ? $_[0]{'name'} = $_[1] : $_[0]{'name'} }
sub root { @_ > 1 ? $_[0]{'root'} = $_[1] : $_[0]{'root'} }
sub config { @_ > 1 ? $_[0]{'config'} = $_[1] : $_[0]{'config'} }
sub ua { @_ > 1 ? $_[0]{'ua'} = $_[1] : $_[0]{'ua'} }

sub logged_in { @_ > 1 ? $_[0]{'state'}{'logged_in'} = $_[1] : $_[0]{'state'}{'logged_in'} }
sub token { @_ > 1 ? $_[0]{'state'}{'token'} = $_[1] : $_[0]{'state'}{'token'} }
sub user_id { @_ > 1 ? $_[0]{'state'}{'user_id'} = $_[1] : $_[0]{'state'}{'user_id'} }

sub init {
    my ($self) = @_;
    my $name = $self->name;
    my $folio = $self->{'folio'};
    my $root = $self->{'root'} = $folio->root . "/site/$name";
    my $config_file = "$root/site.conf";
    die "no config file for $name" if !defined $config_file;
    open my $fh, '<', $config_file or die "open $config_file: $!";
    my $config = $self->{'config'} ||= {};
    my $hash = $config;
    while (<$fh>) {
        next if /^\s*(?:#.*)?$/;  # Skip blank lines and comments
        chomp;
        if (/^\s*\[(.+)\]\s*$/) {
            my $section = lc trim($1);
            $section =~ s/\s+/-/g;
            $hash = $section eq 'general' ? $config : ($config->{$section} ||= {});
        }
        else {
            my ($k, $v) = split /=/, $_, 2;
            die "config syntax: $_" if !defined $v;
            ($k, $v) = (lc trim($k), trim($v));
            $k =~ s/\s+/-/g;
            $hash->{$k} = $v;
        }
    }
    my $uuidmap = $self->{'uuidmap'} ||= {};
    my $uuid_maps_glob = $self->{'uuid-maps'} || 'map/*.uuidmap';
    $uuid_maps_glob = $root.'/'.$uuid_maps_glob
        if $uuid_maps_glob !~ m{^/};
    my @files = glob($uuid_maps_glob);
    foreach my $file (@files) {
        (my $base = $file) =~ s{\.[^.]+$}{};
        $base =~ m{([^/]+)(?:\.[^/.]+)?$}
            or die "invalid UUID map file name: $file";
        my $pfx = $1 . ':';
        open my $fh, '<', $file
            or die "open $file: $!";
        while (<$fh>) {
            next if /^\s*(?:#.*)?$/;  # Skip blank lines and comments
            chomp;
            if (/^(.+)=(.+)$/) {
                my ($alias, $key) = map { $pfx.trim($_) } ($1, $2);
                $uuidmap->{$alias} = $uuidmap->{$key}
                    or die "alias to undefined UUID in $file: $_";
            }
            else {
                my ($uuid, $val) = split /\s+/, $_, 2;
                $uuidmap->{$pfx.$val} = $uuid;
            }
        }
    }
    my $ua = LWP::UserAgent->new;
    $ua->agent("folio/0.1");
    $self->{'ua'} = $ua;
    my $state = eval { $self->state };  # Force reading state if it exists
    $self->state({'logged_in' => 0}) if !$state;
    return $self;
}

sub state {
    my $self = shift;
    my $state_file = $self->root . '/var/state.json';
    my $state =  $self->{'state'};
    if (@_ == 0) {
        return $state if defined $state;
        open my $fh, '<', $state_file
            or die "open $state_file for reading: $!";
        local $/;
        my $str = <$fh>;
        close $fh or die "close $state_file: $!";
        return $self->{'state'} = $json->decode($str);
    }
    elsif (@_ == 1) {
        $state = shift;
        my $str = $json->encode($state);
        open my $fh, '>', $state_file
            or die "open $state_file for writing: $!";
        print $fh $str;
        close $fh or die "close $state_file: $!";
    }
}

sub login {
    my ($self, %arg) = @_;
    my %state = %{ $self->state };
    return if $state{'logged_in'} && $arg{'reuse_token'};
    my $config = $self->config;
    my $res = $self->post('/authn/login', {
        'username' => $config->{'endpoint'}{'user'},
        'password' => $config->{'endpoint'}{'password'},
    }) or die "login failed";
    my $token = $res->header('X-Okapi-Token')
        or die "login didn't yield a token";
    my $content = $json->decode($res->content);
    my $user_id = $content->{'userId'};
    @state{qw(token logged_in user_id)} = ($token, 1, $user_id);
    $self->state(\%state);
    return $self;
}

sub match_users {
    my $self = shift;
    my %result;
    my @clauses;
    foreach my $user (@_) {
        my ($fingerprint, @matchpoints) = $self->_matchpoints('users', $user);
        next if !@matchpoints;
        $result{$fingerprint} = [$user];
        my (@must, @may);
        foreach (@matchpoints) {
            my ($k, $v, $must_match) = @$_;
            if ($must_match) {
                push @must, _cql_term($k, $v);
            }
            else {
                push @may, _cql_term($k, $v);
            }
        }
        push @clauses, _cql_and(
            @must,
            _cql_or(@may)
        );
    }
    my $query = _cql_or(@clauses);
    $query =~ s/\)$// or die "unbalanced parentheses"
        if $query =~ s/^\(//;
    my $res = $self->get("/users?query=$query");
    my $matches = $json->decode($res->content);
    $matches = $matches->{'users'} or return;
    foreach my $match (@$matches) {
        my ($fingerprint, @matchpoints) = $self->_matchpoints('users', $match);
        push @{ $result{$fingerprint} or next }, $match;
    }
    return values %result;
}

sub _cql_term {
    my ($k, $v) = @_;
    $v =~ s/(["()\\])/\\$1/g;
    return qq{$k = "$v"};
}

sub _cql_and {
    return shift if @_ == 1;
    return '(' . join(' and ', @_) . ')';
}

sub _cql_or {
    return shift if @_ == 1;
    return '(' . join(' or ', @_) . ')';
}

sub _matchpoints {
    my ($self, $type, $obj) = @_;
    my $config = $self->config;
    my $root = $self->root;
    my $f = "$root/conf/match-$type.conf";
    my $matching = $config->{'matching'}{$type} ||= $self->_parse_matching($f);
    my $md5 = Digest->new('MD5');
    my @matchpoints;
    while (my ($k, $m) = each %$matching) {
        my $v = _get_attribute_from_dotted($obj, $k);
        next if !defined $v;
        my $must = $m->{'must_match'} ? 1 : 0;
        $md5->add($k, "\n", $v, "\n", $must);
        push @matchpoints, [$k, $v, $must];
    }
    return ($md5->hexdigest, @matchpoints);
}

sub _parse_matching {
    my ($self, $f) = @_;
    open my $fh, '<', $f or die "open $f: $!";
    my (%matching, %protect, %must_match, %overwrite);
    while (<$fh>) {
        next if /^\s*(?:#.*)?$/;  # Skip blank lines and comments
        if (/^match(?:\s+(on|any|lowest|highest|unique|required))?\s+(.+)/) {
            my ($q, $fields) = ($1 || 'any', $2);
            my $qualifier = _matchpoint_qualifier($q);
            foreach my $field (split /,\s*/, $fields) {
                $matching{$field} = {
                    'field' => $field,
                    'qualifier' => $qualifier,
                }
            }
        }
        elsif (/^protect\s+(.+)$/) {
            $protect{$_} = (s/^[-!]// ? 0 : 1) for split /,\s*/, $1;
        }
        elsif (/^must\s+match\s+(.+)$/) {
            $must_match{$_} = 1 for split /,\s*/, $1;
        }
        elsif (/^overwrite\s+(.+)$/) {
            $overwrite{$_} = (s/^[-!]// ? 0 : 1) for split /,\s*/, $1;
        }
        else {
            chomp;
            die "unrecognized line in matching config file $f: $_";
        }
    }
    foreach my $k (keys %protect, keys %overwrite, keys %must_match) {
        $matching{$k} ||= {
            'field' => $k,
        };
    }
    while (my ($k, $v) = each %protect) {
        $matching{$k}{'protect'} = $v;
    }
    while (my ($k, $v) = each %overwrite) {
        $matching{$k}{'overwrite'} = $v;
    }
    while (my ($k, $v) = each %must_match) {
        $matching{$k}{'qualifier'} ||= _matchpoint_qualifier('any');
        $matching{$k}{'must_match'} = $v;
    }
    return \%matching;
}

sub _matchpoint_qualifier {
    my ($q) = @_;
    return $q;
### return sub {
###     my $val = shift;
###     foreach (@_) {
###         return 1 if $_ eq $val;
###     }
###     return 0;
### } if $q eq 'any';
}

sub choose_any { shift @_ }

sub choose_lowest {
    return (sort { $a->{'borrowernumber'} <=> $b->{'borrowernumber'}} @_)[0];
}

sub choose_highest {
    return (sort { $b->{'borrowernumber'} <=> $a->{'borrowernumber'}} @_)[0];
}

sub choose_unique {
    return if @_ != 1;
    return shift;
}

sub _get_attribute_from_dotted {
    my ($obj, $k) = @_;
    while ($k =~ s/^([^.]+)\.(?=[^.])//) {
        return if !$obj;
        my $r = ref $obj;
        if ($r eq 'HASH') {
            $obj = $obj->{$1};
        }
        elsif ($r eq 'ARRAY') {
            return;  # TODO
        }
        else {
            return;
        }
    }
    return $obj->{$k};
}

sub get {
    my $self = shift;
    unshift @_, $self, 'GET';
    goto &req;
}

sub post {
    my $self = shift;
    unshift @_, $self, 'POST';
    goto &req;
}

sub req {
    my ($self, $method, $url, $content) = @_;
    my $config = $self->config;
    my $state = $self->state;
    my $uri = URI->new($config->{'endpoint'}{'uri'} . $url);
    if ($content && ($method eq 'GET' || $method eq 'DELETE')) {
        $uri->query_form(%$content);
    }
    my $req = HTTP::Request->new($method, $uri);
    $req->header('X-Okapi-Tenant' => $config->{'endpoint'}{'tenant'});
    $req->header('Accept' => 'application/json');
    # $req->header('X-Forwarded-For' => '69.43.75.60');
    if ($state->{'logged_in'}) {
        $req->header('X-Okapi-Token' => $state->{'token'});
    }
    if ($method eq 'POST' || $method eq 'PUT') {
        $req->content_type('application/json');
        $req->content($json->encode($content));
    }
    my $ua = $self->ua;
    my $res = $ua->request($req);
    if ($res->is_success) {
        return $res;
    }
    else {
        die "FAIL: $method $url -> ", $res->status_line, "\n";
    }
}

sub trim {
    local $_ = shift;
    s/^\s+|\s+$//g;
    return $_;
}

1;
