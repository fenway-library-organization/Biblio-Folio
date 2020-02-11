package Biblio::Folio::Site;

use strict;
use warnings;

use JSON;
use Digest;
use DBI;
use Text::Balanced qw(extract_delimited);

use Biblio::Folio::Class;
use Biblio::Folio::Object;
use Biblio::Folio::Util;
use Biblio::Folio::Site::Stash;

# Tracing: states
use constant qw(ON       ON     );
use constant qw(OFF      OFF    );
# Tracing: actions
use constant qw(START    START  );
use constant qw(SHOW     SHOW   );
use constant qw(HIDE     HIDE   );
use constant qw(TOGGLE   TOGGLE );
use constant qw(REQUEST  REQUEST);
use constant qw(RESPONSE RESPONSE);

use constant qw(LITERAL LITERAL);
use constant qw(UUID    UUID   );

our $AUTOLOAD;

my $rxuuid = qr/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
my %tok2const = (
    'null' => JSON::null,
    'true' => JSON::true,
    'false' => JSON::false,
);

sub new {
    my $cls = shift;
    unshift @_, 'name' if @_ % 2;
    my $self = bless {
        @_,
        'json' => JSON->new->pretty,
    }, $cls;
    return $self->init;
}

sub folio { @_ > 1 ? $_[0]{'folio'} = $_[1] : $_[0]{'folio'} }
sub name { @_ > 1 ? $_[0]{'name'} = $_[1] : $_[0]{'name'} }
sub root { @_ > 1 ? $_[0]{'root'} = $_[1] : $_[0]{'root'} }
sub ua { @_ > 1 ? $_[0]{'ua'} = $_[1] : $_[0]{'ua'} }
sub json { @_ > 1 ? $_[0]{'json'} = $_[1] : $_[0]{'json'} }

sub config {
    my ($self, $key) = @_;
    my $config = $self->{'config'};
    return $config if @_ == 1;
    return $config->{$key};
}

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

sub _read_config_files {
    my ($self) = @_;
    my @files = (
        $self->file('site.conf'),
        $self->file('conf/*.conf'),
    );
    my %seen;
    my $config = $self->{'config'} ||= {};
    foreach my $file (@files) {
        $file =~ m{/([^/.]+)\.conf$};
        my $name = $1;
        next if $seen{$name}++;
        undef $name if $name eq 'site';
        Biblio::Folio::Util::read_config($file, $self->{'config'}, $name);
    }
}

sub init {
    my ($self) = @_;
    my $name = $self->name;
    my $folio = $self->folio;
    $self->{'root'} = $folio->root . "/site/$name";
    $self->_trace(START);
    $self->_read_config_files;
    $self->_read_map_files;
    $self->_read_cache;
    # $self->_initialize_classes_and_properties;
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

sub _read_cache {
    my ($self) = @_;
    my %cache;
    # TODO
    $self->{'_cached_object'} = \%cache;
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
    my $state_file = $self->file('var/state.json');
    my $state =  $self->{'state'};
    if (@_ == 0) {
        return $state if defined $state;
        open my $fh, '<', $state_file
            or die "open $state_file for reading: $!";
        local $/;
        my $str = <$fh>;
        close $fh or die "close $state_file: $!";
        return $self->{'state'} = $self->json->decode($str);
    }
    elsif (@_ == 1) {
        $state = shift;
        my $str = $self->json->encode($state);
        open my $fh, '>', $state_file
            or die "open $state_file for writing: $!";
        print $fh $str;
        close $fh or die "close $state_file: $!";
    }
}

sub _stash {
    my ($self, $name) = @_;
    my $stashes = $self->{'_stash'};
    return $stashes->{$name}
        if exists $stashes->{$name};
    my $dbh = $self->_dbh;
    return $stashes->{$name} = Biblio::Folio::Site::Stash->new(
        'name' => $name,
        'site' => $self,
        'dbh' => $dbh,
    );
}

sub _dbh {
    my ($self) = @_;
    my $dbfile = $self->file("var/stash/main.sqlite");
    return $self->{'_dbh'}
        ||= DBI->connect("dbi:SQLite:dbname=$dbfile", '', '');
}

sub stash {
    my $self = shift;
    my $name = shift;
    my $table = shift;
    my $stash = $self->_stash($name);
    if (@_ == 0) {
        return;
    }
    elsif (@_ > 1) {
        # @values = $site->stash($name, $table, @columns);
        die "not yet implemented";
    }
    elsif (ref $_[0]) {
        # $site->stash($name, $table, \%hash);
        my %hash = %{ shift() };
        my $id = delete $hash{'id'};
        $stash->put($table, shift());
    }
    else {
        die "bad call";
    }
}

sub login {
    my ($self, %arg) = @_;
    my $state = $self->state;
    delete $state->{'logged_in'} if $arg{'force'};
    return $self if $state->{'logged_in'} && $arg{'reuse_token'};
    my $endpoint = $self->config('endpoint');
    my $res = $self->POST('/authn/login', {
        'username' => $endpoint->{'user'},
        'password' => $endpoint->{'password'},
    }) or die "login failed";
    my $token = $res->header('X-Okapi-Token')
        or die "login didn't yield a token";
    my $content = $self->json->decode($res->content);
    my $user_id = $content->{'userId'};
    @$state{qw(token logged_in user_id)} = ($token, 1, $user_id);
    $self->state($state);
    return $self;
}

sub instance {
    my $self = shift;
    return $self->object('instance', @_);
}

sub location {
    my $self = shift;
    return $self->object('location', @_);
}

sub cached {
    my ($self, $kind, $id) = @_;
    return if !defined $id;
    my $key = $kind . ':' . $id;
    my $cached = $self->{'_cached_object'}{$key};
    my $t = time;
    return $cached->{'object'} if $cached && $cached->{'expiry'} >= $t;
    my $obj = eval { $self->object($kind, $id) };
    my $ttl = $obj->_ttl || 3600;  # One hour
    if ($ttl != -1) {
        $self->{'_cached_object'}{$key} = {
            'object' => $obj,
            'expiry' => $t + $ttl,
        };
    }
    return $obj;
}

sub class {
    my ($self, $pkg) = @_;
    return if $pkg eq '*';
    $pkg = Biblio::Folio::Util::_2pkg($pkg);
    my $class = $self->{'_classes'}{$pkg};
    return $class if $class && $class->is_defined;
    my $kind = Biblio::Folio::Util::_pkg2kind($pkg);
    return $class ||= Biblio::Folio::Class->new(
        'site' => $self,
        'package' => $pkg,
        'kind' => $kind,
        'ttl' => 1,
        'uri' => {},  # XXX
    );
    # $class->define;
}

sub property {
    my ($self, $p) = @_;
    my $prop = $self->{'_properties'}{$p};
    return $prop if $prop;
    die "no such property: $prop";
}

sub object {
    # $site->object($kind, $id);
    # $site->object($kind, \@ids);
    # $site->object($kind, 'query' => $cql, 'limit' => $n, 'offset' => $p);
    # $site->object($kind, 'id' => $id, 'uri' => $uri);
    my ($self, $kind) = splice @_, 0, 2;
    my $pkg = Biblio::Folio::Util::_kind2pkg($kind);
    my (@args, @from, $id, $query);
    if (@_ == 1) {
        my ($arg) = @_;
        my $argref = ref $arg;
        if ($argref eq '') {
            @args = ('id' => $arg);
        }
        elsif ($argref eq 'HASH') {
            @args = ('objects' => [$arg]);
        }
        elsif ($argref eq 'ARRAY') {
            @args = ('objects' => $arg);
        }
    }
    elsif (@_ % 2) {
        die "\$site->object(\$kind, \$id|\@ids\|%args)";
    }
    else {
        my %arg = @args = @_;
        @from = split(/\./, $arg{'from'} || '');
    }
    my @objs = $self->fetch($pkg, @args);
    if (@from) {
        foreach my $k (@from) {
            if ($k eq '*') {
                @objs = map { @$_ } @objs;
            }
            else {
                @objs = map { $_->{$k} } @objs;
            }
        }
    }
    return @objs if wantarray;
    return $objs[0] if @objs == 1;
    return \@objs;
}

sub fetch {
    my ($self, $pkg, %arg) = @_;
    my $uri = delete $arg{'uri'};
    my $objs = delete $arg{'objects'};
    my $id = delete $arg{'id'};
    my $idref = ref $id;
    my $query = $arg{'query'};
    my @return;
    if ($objs) {
        # Just construct the object(s)
        @return = @$objs;
    }
    elsif (defined $query) {
        if (!defined $uri) {
            #die "query without a package or URI: $query"
            #    if !defined $pkg;
            $uri = $pkg->_uri_search || $pkg->_uri;
        }
        #die "search URIs can't contain placeholder %s"
        #    if $uri =~ /%s/;
    }
    else {
        $uri ||= $pkg->_uri;
    }
    if (!$objs && defined $uri) {
        # Really fetch the object
        my %content;
        if (!defined $query && $uri =~ /\?/) {
            $uri = URI->new($uri);
            %content = $uri->query_form;
            $query = $arg{'query'} = $uri->query;
            $uri = $arg{'uri'} = $uri->path;
        }
        else {
            $content{'query'} = $query;
        }
        my ($res, $code);
        if (defined $query) {
            $uri = sprintf($uri, $id)
                if $id && $idref eq '' && $uri =~ /%s/;
            die "search URIs can't contain placeholder %s"
                if $uri =~ /%s/;
            foreach (qw(offset limit)) {
                $content{$_} = $arg{$_} if defined $arg{$_};
            }
            $res = $self->GET($uri, \%content);
            $code = $res->code;
            if ($res->is_success) {
                my $content = $self->json->decode($res->content);
                my @munge = $arg{'munge'} ? @{ $arg{'munge'} } : ();
                @return = $pkg->_search_results($content, @munge);
            }
        }
        elsif (!defined $id) {
            die "I can't fetch a $pkg without a query or an ID";
        }
        else {
            $uri = sprintf($uri, $id);
            $res = $self->GET($uri);
            $code = $res->code;
            if ($res->is_success) {
                my $content = $self->json->decode($res->content);
                @return = (ref($content) eq 'ARRAY' ? @$content : $content);
            }
        }
        #die "unable to execute API call: $uri"
        #    if !defined $code;
        return if $code eq '404';  # Not Found
        die $res->status_line, ' : ', $uri if !$res->is_success;
    }
    else {
        die "can't construct or fetch $pkg objects without data or an ID or query";
    }
    my $instantiate = !$arg{'scalar'} && !$arg{'array'};
    return if !@return;
    return map {
        $pkg->new('_site' => $self, '_json' => $self->json, %$_)
    } @return if $instantiate;
    return @return if wantarray;
    warn "multiple $pkg objects: $uri" if @return > 1;
    return shift @return;
}

### sub _results {
###     my ($self, $content, %arg) = @_;
###     return if !@$content;
###     if (@$content == 1) {
###         $content = $content->[0];
###         my $cref = ref $content;
###         if ($cref eq 'ARRAY') {
###             return map { $pkg->new('_site' => $self, '_json' => $self->json, %$_) } @$content if $instantiate;
###             return @$content;
###         }
###         elsif (wantarray) {
###             return ($content);
###         }
###     }
###     elsif (!defined $arg{'query'} && ref $arg{'id'} eq '') {
###         die "expected one return value, got multiple";
###     }
###     elsif ($instantiate) {
###         @return = $instantiate
###             ? ($pkg->new('_site' => $self, '_json' => $self->json, %$content))
###             : ($content);
###     }
###     return @return if wantarray;
###     return if @return == 0;
###     return \@return if $idref eq 'ARRAY' || defined $query;
###     return $return[0] if @return == 1;
###     die "package $pkg can't return multiple values from $uri if you only asked for one";
### }

### sub property {
###     my ($self, $obj, $name) = @_;
###     my $ref = ref $obj;
###     return if $ref eq '';
###     return $obj->property($name)
###         if $ref =~ /::/ && $obj->can('property');
###     die;
### }

sub objects {
    my ($self, $uri, %arg) = @_;
    my $key = delete $arg{'key'};
    my $res = $self->GET($uri, \%arg);
    return if !$res->is_success;
    my $content = $self->json->decode($res->content);
    my $n = delete $content->{'totalRecords'};
    delete @$content{qw(resultInfo errorMessages)};
    if (!defined $key) {
        my @keys = keys %$content;
        die "which key?" if @keys != 1;
        ($key) = @keys;
    }
    my $objects = $content->{$key};
    die "not an array: $key" if !$objects || ref($objects) ne 'ARRAY';
    return $objects;
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
    my $res = $self->GET('/users', {
        query => $query,
    });
    my $all_matches = $self->json->decode($res->content);
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
    my $matching = $self->config($type)->{'match'} ||= {};
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

sub GET {
    my $self = shift;
    unshift @_, $self, 'GET';
    goto &req;
}

sub POST {
    my $self = shift;
    unshift @_, $self, 'POST';
    goto &req;
}

sub PUT {
    my $self = shift;
    unshift @_, $self, 'PUT';
    goto &req;
}

sub DELETE {
    my $self = shift;
    unshift @_, $self, 'DELETE';
    goto &req;
}

sub req {
    my ($self, $method, $path, $content) = @_;
    my $endpoint = $self->config('endpoint');
    my $state = $self->state;
    my $uri = URI->new($endpoint->{'uri'} . $path);
    if ($content && keys %$content && ($method eq 'GET' || $method eq 'DELETE')) {
        $uri->query_form(%$content);
    }
    my $req = HTTP::Request->new($method, $uri);
    $req->header('X-Okapi-Tenant' => $endpoint->{'tenant'});
    $req->header('Accept' => 'application/json');
    # $req->header('X-Forwarded-For' => '69.43.75.60');
    if ($state->{'logged_in'}) {
        $req->header('X-Okapi-Token' => $state->{'token'});
    }
    if ($method eq 'POST' || $method eq 'PUT') {
        $req->content_type('application/json');
        $req->content($self->json->encode($content));
    }
    _trace($self, REQUEST => $req);
    my $ua = $self->ua;
    my $res = $ua->request($req);
    _trace($self, RESPONSE => $res);
    return $res;
    if (0) {
        if ($res->is_success) {
            return $res;
        }
        else {
            die "FAIL: $method $path -> ", $res->status_line, "\n";
        }
    }
}

sub _trace {
    my $self = shift;
    my $action = shift;
    my $tracing = $self->{'_tracing'}
        or return;
    my $state = $tracing->{'state'} ||= START;
    if ($action eq START) {
        _trace_start($tracing) if $state eq START;
    }
    elsif ($action eq ON) {
        _trace_off($tracing) if $state eq ON;
        _trace_on($tracing, @_);
    }
    elsif ($action eq OFF) {
        _trace_off($tracing);
    }
    elsif ($state eq OFF) {
        return;
    }
    elsif ($action eq REQUEST || $action eq RESPONSE) {
        _trace_message($tracing, $action, @_);
    }
    elsif ($action eq SHOW) {
        my $what = 'show_' . shift;
        $tracing->{$what} = 1;
    }
    elsif ($action eq HIDE) {
        my $what = 'show_' . shift;
        $tracing->{$what} = 0;
    }
    elsif ($action eq TOGGLE) {
        my $what = 'show_' . shift;
        $tracing->{$what} = !$tracing->{$what};
    }
}

sub _trace_start {
}

sub _trace_on {
}

sub _trace_off {
}

sub _trace_write {
    my ($tracing, $str) = @_;
    print STDERR $str;
}

sub _trace_message {
    my ($tracing, $action, $msg) = @_;
    my $str = $action eq REQUEST ? sprintf("> %s %s %s\n", $action, $msg->method, $msg->uri)
                                 : sprintf("< %s %s %s\n", $action, $msg->code, $msg->message);
    _trace_write($tracing, $str);
    if ($tracing->{'show_header'}) {
        _trace_write($tracing, _lines_for_trace($msg->headers_as_string), "\n");
        if ($tracing->{'show_content'}) {
            my $content = eval { $msg->content };
            _trace_write('', _lines_for_trace($content))
                if defined $content && length $content;
        }
    }
}

sub _lines_for_trace {
    my @lines = map { '| ' . $_ . "\n" } map { defined ? (split /\r?\n/) : () } @_;
    if (@lines == 1) {
        substr($lines[0], 0, 1) = '=';
    }
    else {
        substr($lines[0], 0, 1) = '+' if @lines;
        substr($lines[-1], 0, 1) = 'V' if @lines > 1;
    }
    return @lines;
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

sub _camel {
    local $_ = shift;
    s/[-_\s]+(.)/\U$1/g;
    return $_;
}

sub _uncamel {
    local $_ = shift;
    s/(?<=[a-z])(?=[A-Z])/_/g;
    return lc $_;
}

sub _apply_update_to_user {
    my ($self, $existing, $incoming) = @_;
    # Apply changes from $incoming to $existing
    my @changes;
    my $updating = $self->config('users')->{'update'};
    1;  # TODO
    return @changes;
}

sub properties {
    my $self = shift;
    my $val = shift;
    my %val;
    foreach my $ppath (@_) {
        my @p = split /\./, $ppath;
        while (@p) {
            my $p = shift @p;
            my $ref = ref $val;
            $p =~ s/(\[\])?$//;
            my $array = $1;
            if ($ref eq 'HASH') {
                $val = $val->{$p};
            }
            elsif ($ref eq '') {
                undef $val;
            }
            elsif ($ref ne 'ARRAY') {
                $val = $val->$p;
            }
            else {
                die;  # ?
            }
            if ($array) {
                die "not an array: $ppath" if ref($val) ne 'ARRAY';
                my $ppath_under = join('.', @p);
                $val = [ map { $self->properties($_, $ppath_under) } @$val ];
            }
            last if !defined $val;
        }
        $val{$ppath} = $val;
    }
    return \%val;
}

### sub deref_hash_elem {
###     my ($self, $hash, $k, $v) = @_;
###     my $r = ref $v;
###     if ($r eq 'HASH') {
###         $hash->{$k} = $self->deref($v);
###     }
###     elsif ($k =~ /^(.+)Id(s)$/ && $r eq 'ARRAY') {
###         $hash->{$k} = $v;
###         my $kind = $property{$k}{'kind'} || die;
###         my $plural = $property{$k}{'plural'} || $1 . 's';
###         $hash->{$plural} = [ map { ref($_) eq '' && $_ =~ $rxuuid ? $self->deref($self->object($kind, $_)) : $self->deref($_) } @$v ];
###     }
###     elsif ($k =~ /^(.+)Id$/ && $r eq '' && $v =~ $rxuuid) {
###         $hash->{$k} = $v;
###         my $kind = $property{$k}{'kind'} || die;
###         $hash->{$1} = $self->deref($self->object($kind, $v));
###     }
###     elsif ($r eq 'ARRAY') {
###         $hash->{$k} = [ map { $self->deref($_) } @$v ];
###     }
###     return $hash;
### }
### 
### sub deref {
###     my ($self, $val) = @_;
###     my $ref = ref $val;
###     return $val if !$ref;
###     return [ map { $self->deref($_) } @$val ] if $ref eq 'ARRAY';
###     my %h;
###     foreach my $propkey (keys %$val) {
###         my $propval = $val->{$propkey};
###         my $propref = ref $propval;
###         if ($propref eq '' && $propkey =~ /^(.+)Ids?$/ && $propval =~ /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/) {
###             my $propid = $propkey;
###             $propkey = $1;
###             $h{$propid} = $propval;
###             $h{$propkey} = $property{$propid}{'package'}->new($propval);
###         }
###         elsif ($propref eq '') {
###             $h{$propkey} = $propval;
###         }
###         elsif ($propref =~ /::/) {
###             $h{$propkey} = $self->deref($propval);
###         }
###         else {
###             die;
###         }
###     }
###     return \%h;
### }
### 
sub source {
    my $self = shift;
    my $source;
    if (@_ == 1) {
        my $id = shift;
        return $self->object('source' => $id);
    }
    else {
        my %arg = @_;
        if ($arg{'instance'}) {
            my $id = $arg{'instance'};
            my $res = $self->GET('/source-storage/formattedRecords/'.$id, {'identifier' => 'INSTANCE'});
            return if !$res->is_success;
            $source = $self->json->decode($res->content);
        }
        elsif ($arg{'query'}) {
            my $query = $arg{'query'};
            my $res = $self->GET('/source-storage/records', {'query' => $query, 'limit' => 2});
            return if !$res->is_success;
            $source = $res->content->{'records'}
                or return;
            return if ref($source) ne 'ARRAY' || @$source != 1;
            $source = $source->[0];
        }
        else {
            die '$site->source($id|instance=>$id|query=>$cql)';
        }
    }
    return Biblio::Folio::SourceRecord->new('_site' => $self, %$source);
}

sub _initialize_classes_and_properties {
    my ($self) = @_;
    my (%class, %prop2pkg, %prop2class, %blessing);
    my $classes = $self->config('classes')
        or die "no classes configured";
    while (my ($k, $c) = each %$classes) {
        my $kind = $c->{'kind'} ||= _uncamel($k);
        my $pkg = $c->{'package'} ||= Biblio::Folio::Util::_kind2pkg($kind);
        die "class $pkg redefined" if exists $class{$pkg};
        my @refs = split(/,\s*/, delete($c->{'references'}) || '');
        my %uri;
        foreach my $action (qw(fetch base search)) {
            my $uri = $c->{$action}
                or next;
            ($uri{$action} = $uri) =~ s/{[^{}]+}/%s/;
        }
        $c->{'uri'} = \%uri;
        $c->{'references'} = \@refs;
        foreach my $ref (@refs) {
            die "reference property $ref redefined" if exists $prop2class{$ref};
            $prop2pkg{$ref} = $pkg;
        }
        my @blessed_refs;
        foreach (split(/,\s*/, delete($c->{'blessedReferences'}) || '')) {
            /^(\*|[a-z][A-Za-z]*)\.([a-z][A-Za-z]*)(\[\])?$/
                or die "bad blessed reference in class $pkg: $_";
            my ($from_kind, $from_property, $each) = ($1, $2, defined $3);
            my $from_pkg = $from_kind eq '*' ? '*' : Biblio::Folio::Util::_kind2pkg($from_kind);
            my $blessing = {
                'kind' => $kind,
                'package' => $pkg,
                'property' => $from_property,
                'each' => $each,
            };
            push @{ $blessing{$from_pkg} ||= [] }, $blessing;
        }
    }
    while (my ($k, $c) = each %$classes) {
        next if $k eq '*';
        my $pkg = $c->{'package'};
        my @blessings = @{ $blessing{$pkg} || [] };
        my $class = $class{$pkg} = Biblio::Folio::Class->new(
            'site' => $self,
            'blessed_references' => \@blessings,
            %$c,
        );
        my $ok;
        eval { eval "use $pkg"; $ok = 1 };
        if (!$ok) {
            die $@;
        }
    }
    while (my ($p, $pkg) = each %prop2pkg) {
        $prop2class{$p} = $class{$pkg}
            or die "no such class: $pkg";
    }
    $self->{'_classes'} = \%class;
    $self->{'_properties'} = \%prop2class;
}

sub _old_initialize_classes_and_properties {
    my ($self) = @_;
    my (%class, %prop2class, %unresolved);
    # Property name (without Id/Ids)    Definition
    # Key to property definitions:
    #   =PROP   same as PROP (must be the only flag)
    #   +       cached (default TTL)
    #   NUM     cached TTL
    #   auto    auto-instantiated
    #   :CLASS  class to use
    my $properties = q{
        ### UUIDs for short-lived objects:
            courseId                   UUID +1      fetch:/coursereserves/courses/{course_id}
            courseListingId            UUID +1      fetch:/coursereserves/courselistings/{listing_id}
            holdingsRecordId           UUID +1      fetch:/holdings-storage/holdings/{holdingsRecordId}
            instanceId                 UUID +1      fetch:/inventory/instances/{instanceId}
            itemId                     UUID +1      fetch:/item-storage/items/%s
            userId                     UUID +1      fetch:/users/{userId}
        ### Non-properties:
            jobExecutionId             UUID +1      fetch:/change-manager/jobExecutions/{id}
        ### UUIDs for long-lived objects:
            addressTypeId              UUID +3600   fetch:/addresstypes/{addresstypeId}
            alternativeTitleTypeId     UUID +3600   fetch:/alternative-title-types/{id}
            callNumberTypeId           UUID +3600   fetch:/call-number-types/{id}
            campusId                   UUID +3600   fetch:/location-units/campuses/{id}
            classificationTypeId       UUID +3600   fetch:/classification-types/{classificationTypeId}
            contributorNameTypeId      UUID +3600   fetch:/contributor-name-types/{contributorNameTypeId}
            contributorTypeId          UUID +3600   fetch:/contributor-types/{contributorTypeId}
            copyrightStatusId          UUID +3600   fetch:/coursereserves/copyrightstatuses/{status_id}
            courseTypeId               UUID +3600   fetch:/coursereserves/coursetypes/{type_id}
            departmentId               UUID +3600   fetch:/coursereserves/departments/{department_id}
            holdingsNoteTypeId         UUID +3600   fetch:/holdings-note-types/{id}
            holdingsTypeId             UUID +3600   fetch:/holdings-types/{id}
            identifierTypeId           UUID +3600   fetch:/identifier-types/{identifierTypeId}
            illPolicyId                UUID +3600   fetch:/ill-policies/{id}
            instanceFormatId           UUID +3600   fetch:/instance-formats/{instanceFormatId}
            instanceNoteTypeId         UUID +3600   fetch:/instance-note-types/{id}
            instanceRelationshipTypeId UUID +3600   fetch:/instance-relationship-types/{relationshipTypeId}
            instanceTypeId             UUID +3600   fetch:/instance-types/{instanceTypeId}
            institutionId              UUID +3600   fetch:/location-units/institutions/{id}
            itemDamagedStatusId        UUID +3600   fetch:/item-damaged-statuses/{id}
            itemNoteTypeId             UUID +3600   fetch:/item-note-types/{id}
            libraryId                  UUID +3600   fetch:/location-units/libraries/{id}
            locationId                 UUID +3600   fetch:/locations/{id}
            materialTypeId             UUID +3600   fetch:/material-types/{materialtypeId}
            modeOfIssuanceId           UUID +3600   fetch:/modes-of-issuance/{modeOfIssuanceId}
            natureOfContentTermId      UUID +3600   fetch:/nature-of-content-terms/{id}
            permanentLoanTypeId        UUID +3600   fetch:/loan-types/{loantypeId}
            platformId                 UUID +3600   fetch:/platforms/{platformId}
            preferredContactTypeId     UUID +3600   fetch:/no-api/{id}
            processingStatusId         UUID +3600   fetch:/coursereserves/processingstatuses/{status_id}
            scheduleId                 UUID +3600   fetch:/no-api/{id}
            servicePointId             UUID +3600   fetch:/service-points/{servicepointId}
            sourceRecordId             UUID +3600   fetch:/source-storage/records/{id}
            statisticalCodeId          UUID +3600   fetch:/statistical-codes/{statisticalCodeId}
            statisticalCodeTypeId      UUID +3600   fetch:/statistical-code-types/{statisticalCodeTypeId}
            temporaryLoanTypeId        UUID +3600   fetch:/loan-types/{loantypeId}
            termId                     UUID +3600   fetch:/coursereserves/terms/{term_id}
        ### XXX not sure:
            relationshipId             UUID +3600   fetch:/instance-relationship-types/{relationshipTypeId}
            statusId                   UUID +3600   fetch:/instance-statuses/{instanceStatusId}
        ### Literals:
            countryId                  LITERAL
            externalId                 LITERAL
            externalSystemId           LITERAL
            formerId                   LITERAL
            intervalId                 LITERAL
            registerId                 LITERAL
            registrarId                LITERAL
        ### Aliases:
            defaultServicePointId               = servicePointId
            effectiveLocationId                 = locationId
            inTransitDestinationServicePointId  = servicePointId
            itemLevelCallNumberTypeId           = callNumberTypeId
            permanentLocationId                 = locationId
            proxyUserId                         = userId
            servicePointsId                     = servicePointId
            servicepointId                      = servicePointId
            staffMemberId                       = userId
            subInstanceId                       = instanceId
            superInstanceId                     = instanceId
            temporaryLocationId                 = locationId
            typeId                              = callNumberTypeId
    };
    foreach my $propdef (split /\n/, $properties) {
        next if $propdef =~ /^\s*(?:#.*)?$/;  # Skip blank lines and comments
        chomp $propdef;
        $propdef =~ s/^\s+//;
        $propdef =~ s/\s+/ /g;
        my $err = "internal error: unrecognized property definition: $propdef";
        $propdef =~ s/^([A-Za-z]+)// or die $err;
        my $name = $1;
        my @names = $name =~ m{Id/s$} ? ($name, $name.'s') : ($name);
        foreach $name (@names) {
            local $_ = $propdef;
            $unresolved{$name} = $1, next if /^ = (\w+)/;
            my %uri;
            my %p = ('uri' => \%uri);
            my $kind;
            if ($name =~ m{(.+)Ids?$}) {
                $kind = $p{'kind'} = _uncamel($1);
            }
            while (s/^ (?=\S)//) {
                if (s/^[+]([0-9]*|(?= )|$)//) {
                    $p{'ttl'} = length $1 ? $1 : 1;
                    $p{'cache'} = 1;
                }
                elsif (s/^-(?:(?= )|$)//) {
                    $p{'ttl'} = 0;
                }
                elsif (s/^!(\S+)//) {
                    $p{'method'} = $1;
                }
                elsif (s/^%(\S+)//) {
                    $kind = $p{'kind'} = $1;
                }
                elsif (s{^(?:(fetch|base|search):)?(/\S+)}{}) {
                    my $action = $1 || 'fetch';
                    (my $uri = $2) =~ s/{[^{}]+}/%s/;
                    $uri{$action} = $uri;
                }
                elsif (s/^(UUID|LITERAL)//) {
                    $p{'type'} = $1;
                }
                else {
                    die $err;
                }
            }
            my $class;
            if (defined $kind && $p{'type'} ne LITERAL) {
                my $pkg = 'Biblio::Folio::' . ucfirst _camel($kind);
                $p{'package'} = $pkg;
                $class = $class{$pkg} = {
                    'site' => $self,
                    'name' => $name,
                    %p,
                };
            }
            $prop2class{$name} = Biblio::Folio::Class->new(
                'site' => $self,
                'name' => $name,
                %p,
            );
        }
    }
    my $n = 5;
    while (keys(%unresolved) && $n--) {
        foreach my $alias (keys %unresolved) {
            my $name = $unresolved{$alias};
            if (exists $prop2class{$name}) {
                $prop2class{$alias} = $prop2class{$name};
                delete $unresolved{$alias};
            }
        }
    }
    my @unresolved = sort keys %unresolved;
    die "unresolved property aliases: @unresolved" if @unresolved;
    $self->{'_properties'} = \%prop2class;
    $self->{'_classes'} = \%class;
    # $self->_define_classes(values %class);
}

### sub Biblio::Folio::Location::_uri       { '/locations/%s' }
### sub Biblio::Folio::CallNumberType::_uri { '/call-number-types/%s' }
### sub Biblio::Folio::LoanType::_uri       { '/loan-types/%s' }
### sub Biblio::Folio::Institution::_uri    { '/location-units/institutions/%s' }
### sub Biblio::Folio::Campus::_uri         { '/location-units/campuses/%s' }
### sub Biblio::Folio::Library::_uri        { '/location-units/libraries/%s' }

sub TO_JSON {
    my %self = %{ shift() };
    delete @self{grep { /^_/ || ref($self{$_}) !~ /^(?:ARRAY|HASH)$/ } keys %self};
    return \%self;
}

sub DESTROY { }

sub AUTOLOAD {
    # $site->user($user_id) --> $site->object('user', $user_id);
    # $site->campus('query' => 'name = "Riverside Campus"');
    my $self = shift;
    die if !@_;
    (my $kind = $AUTOLOAD) =~ s/.*:://;
    my $pkg = ucfirst _camel($kind);
    my $class = eval { $self->class($pkg) };
    if ($class) {
        my $cache = $class->{'cache'};
        unshift @_, $kind;
        if ($cache && @_ == 2) {
            goto &cached;
        }
        else {
            goto &object;
        }
    }
    else {
        die $@;
    }
}

1;
