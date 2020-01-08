package Biblio::Folio::Site;

use strict;
use warnings;

use JSON;
use Digest;
use Text::Balanced qw(extract_delimited);

my $json = JSON->new->pretty;
my %tok2const = (
    'null' => JSON::null,
    'true' => JSON::true,
    'false' => JSON::false,
);

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

sub file {
    my ($self, $path) = @_;
    $path = $self->{'root'} . '/' . $path if $path !~ m{^/};
    my @files = glob($path);
    return @files if wantarray;
    die "multiple files for $path" if @files > 1;
    return if !@files;
    return $files[0];
}

sub _read_config {
    my ($self, $file, $key) = @_;
    $file = $self->file($file);
    my $hash = my $config = $self->{'config'} ||= {};
    $hash = $config = $hash->{$key} ||= {} if defined $key;
    open my $fh, '<', $file or die "open $file: $!";
    while (<$fh>) {
        next if /^\s*(?:#.*)?$/;  # Skip blank lines and comments
        chomp;
        if (/^\s*\[(.+)\]\s*$/) {
            my $section = trim($1);
            $section =~ s/\s+/-/g;
            $hash = $section eq 'general' ? $config : ($config->{$section} ||= {});
        }
        else {
            my ($k, $v) = split /=/, $_, 2;
            die "config syntax: $_" if !defined $v;
            ($k, $v) = (camelize(trim($k)), trim($v));
            $k =~ s/\s+/-/g;
            $hash->{$k} = $v;
        }
    }
}

sub _read_config_files {
    my ($self) = @_;
    my $config_file = $self->file('site.conf');
    $self->_read_config('site.conf');
    foreach my $file ($self->file('conf/*.conf')) {
        $file =~ m{/([^/.]+)\.conf$};
        $self->_read_config($file, $1);
    }
}

sub init {
    my ($self) = @_;
    my $name = $self->name;
    my $folio = $self->{'folio'};
    $self->{'root'} = $folio->root . "/site/$name";
    $self->_read_config_files;
    $self->_read_map_files;
    $self->_build_matching($_) for qw(users);
    my $ua = LWP::UserAgent->new;
    $ua->agent("folio/0.1");
    $self->{'ua'} = $ua;
    my $state = eval { $self->state };  # Force reading state if it exists
    $self->state({'logged_in' => 0}) if !$state;
    return $self;
}

sub _read_map_files {
    my ($self) = @_;
    my %uuidmap;
    my %uuidunmap;
    my $uuid_maps_glob = $self->{'uuid-maps'} || 'map/*.uuidmap';
    my @files = $self->file($uuid_maps_glob);
    foreach my $file (@files) {
        (my $base = $file) =~ s{\.[^.]+$}{};
        $base =~ m{([^/]+)(?:\.[^/.]+)?$}
            or die "invalid UUID map file name: $file";
        my $name = $1;
        open my $fh, '<', $file
            or die "open $file: $!";
        while (<$fh>) {
            next if /^\s*(?:#.*)?$/;  # Skip blank lines and comments
            chomp;
            if (/^(.+)=(.+)$/) {
                my ($alias, $key) = map { trim($_) } ($1, $2);
                my $val = $uuidmap{$name}{$key};
                $uuidmap{$name}{$alias} = $val
                    or die "alias to undefined UUID in $file: $_";
                $uuidunmap{$name}{$val} ||= $alias;
            }
            elsif (/^([^:]+)(?:\s+:\s+(.+))?$/) {
                my ($key, $val) = ($1, $2);
                $val = $key if !defined $val;
                $uuidmap{$name}{$val} = $key;
                $uuidunmap{$name}{$key} ||= $val;
            }
        }
    }
    $self->{'uuidmap'} = \%uuidmap;
    $self->{'uuidunmap'} = \%uuidunmap;
}

sub decode_uuid {
    my ($self, $name, $uuid) = @_;
    return $self->{'uuidunmap'}{$name}{$uuid};
}

sub expand_uuid {
    my ($self, $name, $uuid) = @_;
    my $expanded = $self->{'uuidunmap'}{$name}{$uuid};
    return $uuid if !defined $expanded;
    return "$uuid <$expanded>";
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

sub _build_matching {
    my ($self, $type) = @_;
    my $matching = $self->{'config'}{$type}{'match'} ||= {};
    foreach my $k (keys %$matching) {
        my %fm = (
            'field' => $k,
            'qualifier' => 'any',
            'required' => 0,
            'order' => 1<<15,
            'exact' => 0,
        );
        local $_ = $matching->{$k};
        my $n = 0;
        while (/\S/) {
            if ($n++) {
                s/^\s*,\s+// or die "invalid matching parameters: $k = $matching->{$k}";
            }
            if (s/^copy from (\w+)//) {
                $fm{'copy_from'} = $1;
            }
            elsif (s/^default://) {
                my $dv = extract_value();
                die "invalid default: $_" if !defined $dv;
                $fm{'default'} = $dv;
            }
            elsif (s/^([^:,\s]+)//) {
                my ($pk, $pv) = ($1, 1);
                if (s/^:\s*//) {
                    $pv = extract_value();
                    die "invalid value for $pk: $_" if !defined $pv;
                }
                # if ($pk =~ /^(lowest|highest)$/) {
                #     ($pk, $pv) = ('qualifier', _matchpoint_qualifier($pk));
                # }
                $fm{$pk} = $pv;
            }
        }
        die "contradictory mode for matching $type: field $k both required and optional"
            if $fm{'required'} && $fm{'optional'};
        $matching->{$k} = \%fm;
    }
}

sub extract_value {
    # This function operates (destructively) on $_
    my $v = extract_delimited(undef, q{"'}, '\s*');
    if (defined $v) {
        $v =~ /^(["'])(.*)$1$/ or die "wtf?";
        $v = $2;
    }
    elsif (s/^(true|false|null)//) {
        $v = $tok2const{$1};
    }
    elsif (s/^([0-9]+(?:\.[0-9]+)?)//) {
        $v = $1;
    }
    elsif (s/^(\w+)//) {
        $v = $1;
    }
    return $v;
}

sub match_users {
    my $self = shift;
    my @clauses;
    my %has_term;
    my %match;
    my @results;
    foreach my $u (0..$#_) {
        my $user = $_[$u];
        $match{$u} = {};
        my @mp = $self->_matchpoints('users', $user);
        push @results, {
            'user' => $user,
            'matches' => [],
        };
        next if !@mp;
        my (@may_terms, @must_terms);
        foreach (@mp) {
            my ($k, $v, $mp) = @$_;
            my $term = _cql_term($k, $v, $mp);
            $has_term{$u}{$term} = 1;
            if ($mp->{'required'}) {
                push @must_terms, $term;
            }
            else {
                push @may_terms, $term;
            }
        }
        next if !@may_terms;  # All we have left is required terms -- we can't build a determinative query
        push @clauses, _cql_and(
            @must_terms,
            _cql_or(@may_terms)
        );
    }
    return @results if !@clauses;
    my $query = _cql_or(@clauses);
    $query =~ s/\)$// or die "unbalanced parentheses"
        if $query =~ s/^\(//;
    my $res = $self->get('/users', {
        query => $query,
    });
    my $all_matches = $json->decode($res->content);
    $all_matches = $all_matches->{'users'} or return @results;
    # We have at least one matching user in FOLIO
    foreach my $m (0..$#$all_matches) {
        my $match = $all_matches->[$m];
        my @mp = sort {
            my ($A, $B) = ($a->[-1], $b->[-1]);
            my ($afld, $areq, $aord) = @$A{qw(field required order)};
            my ($bfld, $breq, $bord) = @$B{qw(field required order)};
            $areq <=> $breq || $aord <=> $bord || $afld cmp $bfld
        } $self->_matchpoints('users', $match);
        foreach (@mp) {
            my ($k, $v, $mp) = @$_;
            my $term = _cql_term($k, $v, $mp);
            my $required = $mp->{'required'};
            foreach my $u (0..$#_) {
                if (!$has_term{$u}{$term}) {
                    # Not a match by $term -- not a match at all, if $term is required
                    delete $match{$u}{$m} if $required;
                }
                elsif (!$required) {
                    # A match by an optional matchpoint -- this is where we "connect the dots"
                    $match{$u}{$m} = 1;
                    push @{ $results[$u]{'matches'}[$m]{'by'} ||= [] }, $mp->{'field'};
                }
                else {
                    # A match by a required matchpoint -- no need to do anything
                    1;
                }
            }
        }
        foreach my $u (0..$#_) {
            if ($match{$u}{$m}) {
                $results[$u]{'matches'}[$m]{'user'} = $match;
            }
###         else {
###             $matches->[$m] ||= {};
###             # $results[$u]{'matches'}[$m] = 0;
###         }
        }
    }
    foreach my $result (@results) {
        @{ $result->{'matches'} } = grep { defined $_ } @{ $result->{'matches'} };
    }
    return @results;
}

sub _cql_term {
    my ($k, $v, $mp) = @_;
    $v =~ s/(["()\\\*\?])/\\$1/g;
    return qq{$k == "$v"} if $mp->{'exact'};
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
    my $matching = $self->config->{$type}{'match'} ||= {};
    my @mp;
    while (my ($k, $mp) = each %$matching) {
        my $v = _get_attribute_from_dotted($obj, $mp->{'from'} || $k);
        push @mp, [$k, $v, $mp] if defined $v && length $v;
    }
    return @mp;
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

sub norm {
    local $_ = trim(shift);
    s/\s+/ /g;
    return $_;
}

sub camelize {
    local $_ = shift;
    s/\s+(.)/\U$1/g;
    return $_;
}

sub _apply_update_to_user {
    my ($self, $existing, $incoming) = @_;
    # Apply changes from $incoming to $existing
    my @changes;
    my $updating = $self->config->{'users'}{'update'};
    1;  # TODO
    return @changes;
}

1;

