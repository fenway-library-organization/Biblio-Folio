package Biblio::Folio::Site;

use strict;
use warnings;

use JSON;
use Digest;
use DBI;
use Text::Balanced qw(extract_delimited);

use Biblio::Folio::Class;
use Biblio::Folio::Object;
use Biblio::Folio::Util qw(_read_config _2pkg _pkg2kind _kind2pkg _optional _use_class _cql_term _cql_or);
use Biblio::Folio::Site::Searcher;
use Biblio::Folio::Site::Stash;
use Biblio::Folio::Site::LoadProfile;
use Biblio::Folio::Site::Matcher;
use Biblio::Folio::Site::BatchLoader;

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
sub cache { @_ > 1 ? $_[0]{'cache'} = $_[1] : $_[0]{'cache'} }
sub dont_cache { @_ > 1 ? $_[0]{'dont_cache'} = $_[1] : $_[0]{'dont_cache'} }

sub config {
    my ($self, $key) = @_;
    my $config = $self->{'config'};
    return $config if @_ == 1;
    return $config->{$key};
}

sub logged_in { @_ > 1 ? $_[0]{'state'}{'logged_in'} = $_[1] : $_[0]{'state'}{'logged_in'} }
sub token { @_ > 1 ? $_[0]{'state'}{'token'} = $_[1] : $_[0]{'state'}{'token'} }
sub user_id { @_ > 1 ? $_[0]{'state'}{'user_id'} = $_[1] : $_[0]{'state'}{'user_id'} }

# Record load profiles

# my $profiles = $site->load_profiles;
sub load_profiles {
    my ($self, $type) = @_;
    my $profiles = $self->{'load_profile'}{$type};
    if (!$profiles) {
        $profiles = {};
        my @files = $self->file("profile/$type/*.profile");
        foreach my $file (@files) {
            (my $name = $file) =~ s{^.+/|\.profile$}{}g;
            my $profile = _read_config($file);
            $profiles->{$name} = Biblio::Folio::Site::LoadProfile->new(
                'name' => $name,
                'type' => $type,
                %$profile,
            );
        }
        $self->{'load_profile'}{$type} = $profiles;
    }
    return $profiles;
}

# my $profile = $site->load_profile('user');
# my $profile = $site->load_profile('user', 'default');
sub load_profile {
    my ($self, $kind, $name) = @_;
    $name = 'default' if !defined $name;
    return $self->load_profiles($kind)->{$name}
        || die "no such profile for $kind: $name";
}

sub compile_profile {
    my ($self, $profile) = @_;
    1;  # TODO
}

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
        _read_config($file, $self->{'config'}, $name);
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
    $self->cache(\%cache);
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
    my $username = $endpoint->{'user'};
    my $res = $self->POST('/authn/login', {
        'username' => $username,
        'password' => $endpoint->{'password'},
    }) or die "login failed";
    my $token = $res->header('X-Okapi-Token')
        or die "login didn't yield a token";
    @$state{qw(token logged_in)} = ($token, 1);
    my $user = $self->object('user', 'query' => qq{username == "$username"});
    $state->{'user_id'} = $user->id;
    $self->state($state);
    return $self;
}

sub authenticate {
    my ($self, %arg) = @_;
    my ($user, $password) = @arg{qw(username password)};
    my $endpoint = $self->config('endpoint');
    my $res = $self->POST('/authn/login', {
        'username' => $user,
        'password' => $password,
    });
    return if !$res->is_success;
    my $token = $res->header('X-Okapi-Token')
        or die "login succeeded but no token was returned";
    my $content = eval { $self->json->decode($res->content) }
        or die "login succeeded but no user data was returned";
    return {
        'token' => $token,
        %$content,
    };
}

sub instance {
    my $self = shift;
    return $self->object('instance', @_);
}

sub location {
    my $self = shift;
    return $self->object('location', @_);
}

sub all {
    my ($self, $kind) = @_;
    return $self->objects($kind, 'limit' => 1<<31);
}

sub cached {
    my ($self, $kind, $what) = @_;
    return if !defined $what;
    my ($obj, $id);
    if (!defined $what) {
        return;
    }
    elsif (ref $what) {
        $obj = $what;
        $id = $obj->{'id'}
            or return $what;
    }
    else {
        $id = $what;
    }
    if ($self->dont_cache->{$kind}) {
        return $obj || $self->object($kind, $id);
    }
    my $key = $kind . ':' . $id;
    my $t = time;
    my $cache = $self->cache;
    my $cached = $cache->{$key};
    if (!$obj) {
        return $cached->{'object'} if $cached && $cached->{'expiry'} >= $t;
        $obj = $self->object($kind, $id);
    }
    my $ttl = $obj->_ttl || 3600;  # One hour
    if ($ttl == -1) {
        delete $cache->{$key} if $cached;
    }
    elsif (!$cached || $cached->{'expiry'} < $t) {
        $cache->{$key} = {
            'object' => $obj,
            'expiry' => $t + $ttl,
        };
    }
    return $obj;
}

sub class {
    my ($self, $pkg) = @_;
    return if $pkg eq '*';
    $pkg = _2pkg($pkg);
    my $class = $self->{'_classes'}{$pkg};
    return $class if $class && $class->is_defined;
    my $kind = _pkg2kind($pkg);
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

sub searcher {
    # $searcher = $site->searcher($kind);
    # $searcher = $site->searcher($kind, '@limit' => $limit);
    # $searcher = $site->searcher($kind, 'id' => [@ids], 'active' => true);
    # $searcher = $site->searcher($kind, '@limit' => 20, 'id' => [@ids], '@offset' => 100);
    # $searcher->limit(100);
    # while (my $object = $searcher->next) {
    #     print $object->id, "\n";
    # }
    my ($self, $kind, %term) = @_;
    # Look for offset and limit parameters
    my %param;
    foreach my $k (keys %term) {
        if ($k =~ /^\@(.+)/) {
            $param{$1} = delete $term{$k};
        }
    }
    die "unrecognized argument" if @_ % 2;
    return Biblio::Folio::Site::Searcher->new(
        'site' => $self,
        'kind' => $kind,
        %param,
        %term ? ('terms' => \%term) : (),
    );
}

### sub query {
###     # $site->query($kind, 'id' => \@ids, 'active' => true, [$offset, $limit]);
###     my $self = shift;
###     my $kind = shift;
###     my @terms;
###     my $exact = { 'exact' => 1 };
###     my ($cql, @terms, $offset, $limit, %arg);
###     if (@_ > 1 && $_[0] eq 'cql') {
###         shift;
###         $arg{'query'} = shift;
###     }
###     else {
###         while (@_ > 1) {
###             my ($k, $v) = splice @_, 0, 2;
###             if (ref($v) eq 'ARRAY') {
###                 push @terms, _cql_term($k, $v, $exact, $k =~ /id$/i);
###             }
###             else {
###                 push @terms, _cql_term($k, $v, $exact);
###             }
###         }
###         $arg{'query'} = _cql_and(@terms);
###     }
###     if (@_ == 1) {
###         @arg{qw(offset limit)} = @{ shift() };
###     }
###     return $self->object($kind, %arg);
### }

sub object {
    # $site->object($kind, $id);
    # $site->object($kind, $object_data);
    # $site->object($kind, \@ids);
    # $site->object($kind, 'query' => $cql, 'limit' => $n, 'offset' => $p);
    # $site->object($kind, 'id' => $id, 'uri' => $uri);
    my ($self, $kind) = splice @_, 0, 2;
    my $pkg = _kind2pkg($kind);
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
    @objs = map { $self->cached($kind, $_) } @objs;
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
    my $burst;
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
        else {
            if (defined $id) {
                $uri = sprintf($uri, $id);
                $res = $self->GET($uri);
            }
            else {
                $burst = 1;
                foreach (qw(offset limit)) {
                    $content{$_} = $arg{$_} if defined $arg{$_};
                }
                delete $content{'query'};
                $uri =~ s{/%s$}{};  # or die "I don't know how to fetch a $pkg using URI $uri without a query or an ID";
                $res = $self->GET($uri, \%content);
            }
            $code = $res->code;
            if ($res->is_success) {
                my $content = $self->json->decode($res->content);
                @return = $burst ? _burst($content, $arg{'key'})
                                 : (ref($content) eq 'ARRAY' ? @$content : $content);
            }
        }
        #die "unable to execute API call: $uri"
        #    if !defined $code;
        return if $code eq '404';  # Not Found
        die $res->status_line, ' : ', $uri if !$res->is_success;
    }
    elsif (!@return) {
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

sub create {
    my ($self, $kind, %arg) = @_;
    my $pkg = _kind2pkg($kind);
    my $uri = delete $arg{'uri'} || $pkg->_uri_create || $pkg->_uri
        or die "no URI to create a $pkg";
    my $res = $self->POST($uri, \%arg);
    return if !$res->is_success;
    my $json = $self->json;
    my $content = eval {
        $json->decode($res->content);
    };
    if (!$content) {
        return if $res->code ne '201';
        $content = \%arg;
    }
    return $pkg->new('_site' => $self, '_json' => $json, %$content);
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
    my ($self, $kind, %arg) = @_;
    my $pkg = _kind2pkg($kind);
    my $uri = $pkg->_uri_search || $pkg->_uri;
    my $key = delete $arg{'key'};
    my $res = $self->GET($uri, \%arg);
    return if !$res->is_success;
    my $content = $self->json->decode($res->content);
    my @elems = _burst($content, $arg{'key'});
    return map {
        $pkg->new('_site' => $self, '_json' => $self->json, %$_)
    } @elems;
}

sub _burst {
    my ($content, $key) = @_;
    my $n = delete $content->{'totalRecords'};
    delete @$content{qw(resultInfo errorMessages)};
    if (!defined $key) {
        my @keys = keys %$content;
        die "which key?" if @keys != 1;
        ($key) = @keys;
    }
    my $elems = $content->{$key};
    die "not an array: $key" if !$elems || ref($elems) ne 'ARRAY';
    return @$elems;
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

sub make_request {
    my ($self, $method, $what, $content) = @_;
    my $endpoint = $self->config('endpoint');
    my $state = $self->state;
    my $r = ref $what;
    my ($path, $uri, $req);
    if ($r eq '') {
        $uri = URI->new($endpoint->{'uri'} . $what);
        if ($content && keys %$content && ($method eq 'GET' || $method eq 'DELETE')) {
            $uri->query_form(%$content);
        }
        $req = HTTP::Request->new($method, $uri);
    }
    else {
        die "attempt to request a $r"
            if !$what->can('uri');
        $req = $what;
        $uri = $req->uri;
        $uri = URI->new($uri) if !ref $uri;
        $path = $uri->path;
    }
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
    return $req;
}

sub req {
    my ($self, $method, $what, $content) = @_;
    my $req = $self->make_request($method, $what, $content);
    _trace($self, REQUEST => $req);
    my $ua = $self->ua;
    my $res = $ua->request($req);
    _trace($self, RESPONSE => $res);
    return $res;
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

sub update_object {
    my ($self, %arg) = @_;
    my ($object, $using, $profile) = @arg{qw(object using profile)};
    # Apply changes from $using to $object
    my @changes;
    my $fields = $profile->{'fields'};
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
    return Biblio::Folio::Object::SourceRecord->new('_site' => $self, %$source);
}

sub source_records {
    my ($self, @iids) = @_;
    my $searcher = $self->searcher('source_record', 'externalIdsHolder.instanceId' => \@iids, '@limit' => scalar @iids);
    return $searcher->all;
}

sub search {
    my ($self, $uri, $cql, %arg) = @_;
    my $results = eval {
        my $res = $self->GET($uri, {
            'query' => $cql,
            _optional('offset' => $arg{'offset'}),
            _optional('limit' => $arg{'limit'}),
            _optional('order_by' => $arg{'order_by'}),
        });
        $self->json->decode($res->content);
    } or die "search failed: $cql";
    return $results;
}

sub _initialize_classes_and_properties {
    my ($self) = @_;
    my (%class, %prop2pkg, %prop2class, %blessing);
    my $classes = $self->config('classes')
        or die "no classes configured";
    while (my ($k, $c) = each %$classes) {
        my $kind = $c->{'kind'} ||= _uncamel($k);
        my $pkg = $c->{'package'} ||= _kind2pkg($kind);
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
            my $from_pkg = $from_kind eq '*' ? '*' : _kind2pkg($from_kind);
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

sub marc2instance {
    my ($self, $marcref, $mapping) = @_;
    my $conv = $self->{'_marc2instance'} ||= $self->_make_marc2instance($mapping);
    return $conv->($marcref);
}

sub _make_marc2instance {
    use MARC::Loop qw(marcparse TAG VALREF SUBS);
    my ($self, $mapping) = @_;
    my $status_id = $self->object('instance_status', 'query' => 'code=="batch"')->id;
    my $folio = $self->folio;
    my $json = $self->json;
    my %rule;
    foreach my $tag (keys %$mapping) {
        my $fld_conf = $mapping->{$tag};
        my $ent = $fld_conf->[0]{'entity'};
        my @data_elements = $ent ? @$ent : ($fld_conf);
        foreach my $elem (@data_elements) {
            1;
        }
    }
    return sub {
        my ($marc) = @_;
        my ($leader, $fields) = ($marc->leader, $marc->fields);
        my %instance = (
            id => undef,
            alternativeTitles => [],
            editions => [],
            series => [],
            identifiers => [],
            contributors => [],
            subjects => [],
            classifications => [],
            publication => [],
            publicationFrequency => [],
            publicationRange => [],
            electronicAccess => [],
            instanceFormatIds => [],
            physicalDescriptions => [],
            languages => [],
            notes => [],
            staffSuppress => JSON::false,
            discoverySuppress => JSON::false,
            statisticalCodeIds => [],
            tags => {},
            holdingsRecords2 => [],
            natureOfContentTermIds => [],
            statusId => $status_id,
        );
        my $f999 = $marc->field(sub {
            $_->tag eq '999' && $_->indicators eq 'ff'
        });
        if ($f999) {
            my $i = $f999->subfield('i');
            $instance{'id'} = $i if defined $i;
        }
        my $f001 = $marc->field('001');
        if ($f001) {
            my $h = $f001->contents;
            $instance{'hrid'} = $1 if defined $h;
        }
        foreach my $field (@$fields) {
            my $tag = $field->tag;
            my $rule = $rule{$tag} or next;
            1;
        }
        $instance{'id'} ||= $folio->uuid;
        return $json->encode(\%instance);
    };
}

sub formatter {
    my ($self, $kind, %arg) = @_;
    my $pkg = _kind2pkg($kind);
    return $pkg->formatter(%arg);
}

sub process_file {
    my ($self, $kind, $file, %arg) = @_;
    my $parser = $self->parser_for($kind, $file, %arg);
    $parser->iterate(%arg);
}

sub parser_for {
    my ($self, $kind, $file, %arg) = @_;
    my $profile = $self->load_profile($kind, $arg{'profile'});
    my %parser = %{ $profile->{'parser'} || {} };
    my $parser_cls = delete $parser{'class'} || 'Biblio::FolioX::Util::JSONParser';
    $parser_cls = 'Biblio::FolioX::' . $parser_cls if $parser_cls =~ s/^[+](::)?//;
    _use_class($parser_cls);
    return $parser_cls->new(
        %parser,
        'site' => $self,
        'profile' => $profile,
        'kind' => $kind,
        'file' => $file,
    );
}

sub matcher_for {
    my ($self, $kind, $file, %arg) = @_;
    my $profile = $self->load_profile($kind, $arg{'profile'});
    my %matcher = %{ $profile->{'matcher'} || {} };
    my $matcher_cls = delete $matcher{'class'} || 'Biblio::Folio::Site::Matcher';
    $matcher_cls = 'Biblio::FolioX::' . $matcher_cls if $matcher_cls =~ s/^[+](::)?//;
    _use_class($matcher_cls);
    return $matcher_cls->new(
        %matcher,
        'site' => $self,
        'profile' => $profile,
        'kind' => $kind,
        'file' => $file,
    );
}

sub loader_for {
    my ($self, $kind, $file, %arg) = @_;
    my $profile = $self->load_profile($kind, $arg{'profile'});
    my %loader = %{ $profile->{'loader'} || {} };
    my $loader_cls = delete $loader{'class'} || 'Biblio::Folio::Site::BatchLoader';
    $loader_cls = 'Biblio::FolioX::' . $loader_cls if $loader_cls =~ s/^[+](::)?//;
    _use_class($loader_cls);
    return $loader_cls->new(
        %loader,
        'site' => $self,
        'profile' => $profile,
        'kind' => $kind,
        'file' => $file,
    );
}

### sub matcher {
###     my ($self, $kind, $file, %arg) = @_;
###     my $p = delete $arg{'profile'};
###     my $profile = $self->load_profile($kind, $p);
###     return Biblio::Folio::Site::Matcher->new(
###         'site' => $self,
###         'kind' => $kind,
###         'file' => $file,
###         'profile' => $profile,
###         %arg,
###     );
### }

### sub loader {
###     my ($self, $kind, %arg) = @_;
###     my $p = delete $arg{'profile'};
###     my $profile = $self->load_profile($kind, $p);
###     my $loader_cls = $profile->{'loader'}{'class'}
###         or die "no batch loader class defined for $kind objects";
###     $loader_cls = 'Biblio::FolioX::' . $loader_cls if $loader_cls =~ s/^[+](::)?//;
###     _use_class($loader_cls);
###     return $loader_cls->new(
###         'site' => $self,
###         'kind' => $kind,
###         'profile' => $profile,
###         %arg,
###     );
### }

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
        unshift @_, $self, $kind;
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
