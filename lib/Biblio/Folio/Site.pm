package Biblio::Folio::Site;

use strict;
use warnings;

use JSON;
use Digest;
use DBI;
use Text::Balanced qw(extract_delimited);
use LWP::UserAgent;
use POSIX qw(strftime);

use Biblio::Folio::Class;
use Biblio::Folio::Object;
use Biblio::Folio::Site::Searcher;
use Biblio::Folio::Site::LocalDB;
use Biblio::Folio::Site::LocalDB::Instances;
use Biblio::Folio::Site::Profile;
use Biblio::Folio::Site::Matcher;
use Biblio::Folio::Site::BatchLoader;
use Biblio::Folio::Util qw(
    _json_encode _json_decode _json_read _json_write
    _read_config _optional _uuid _utc_datetime _mkdirs
    _2pkg _pkg2kind _kind2pkg _use_class
    _cql_query _cql_term _cql_or
);

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
    my $self = bless { @_ }, $cls;
    return $self->init;
}

sub folio { @_ > 1 ? $_[0]{'folio'} = $_[1] : $_[0]{'folio'} }
sub name { @_ > 1 ? $_[0]{'name'} = $_[1] : $_[0]{'name'} }
sub root { @_ > 1 ? $_[0]{'root'} = $_[1] : $_[0]{'root'} }
sub ua { @_ > 1 ? $_[0]{'ua'} = $_[1] : $_[0]{'ua'} }
sub json { @_ > 1 ? $_[0]{'json'} = $_[1] : $_[0]{'json'} }
sub cache { @_ > 1 ? $_[0]{'cache'} = $_[1] : $_[0]{'cache'} }

sub init {
    my ($self) = @_;
    my $name = $self->name;
    my $folio = $self->folio;
    $self->{'root'} = $folio->root . "/site/$name";
    $self->{'dont_cache'} ||= {};
    $self->_trace(START);
    $self->_read_config_files;
    $self->_read_map_files;
    $self->_read_cache;
    my $ua = LWP::UserAgent->new;
    $ua->agent("folio/0.1");
    $self->{'ua'} = $ua;
    my $state = eval { $self->state };  # Force reading state if it exists
    $self->state({'logged_in' => 0}) if !$state;
    return $self;
}

sub dont_cache {
    my $self = shift;
    my $dont_cache = $self->{'dont_cache'};
    $dont_cache->{$_} = 1 for @_;
    return $dont_cache;
}

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

# my $profiles = $site->profiles;
sub profiles {
    my ($self, $kind) = @_;
    my $profiles = $self->{'profile'}{$kind};
    if (!$profiles) {
        $profiles = {};
        my @files = $self->file("profile/$kind/*.profile");
        foreach my $file (@files) {
            (my $name = $file) =~ s{^.+/|\.profile$}{}g;
            my $profile = _read_config($file);
            my $pkg = $profile->{'profile'}{'class'} || 'Biblio::Folio::Site::Profile';
            _use_class($pkg);
            $profiles->{$name} = $pkg->new(
                'kind' => $kind,
                'name' => $name,
                %$profile,
            );
        }
        $self->{'profile'}{$kind} = $profiles;
    }
    return $profiles;
}

# my $profile = $site->profile('user');
# my $profile = $site->profile('user', 'default');
# my $profile = $site->profile('user', $profile_object);
sub profile {
    my ($self, $kind, $name) = @_;
    return $name if ref $name;
    $name = 'default' if !defined $name;
    return $self->profiles($kind)->{$name}
        || die "no such profile for $kind: $name";
}

sub compile_profile {
    my ($self, $profile) = @_;
    1;  # TODO
}

sub path {
    my ($self, $path) = @_;
    return $path if $path =~ m{^/};
    return $self->{'root'} . '/' . $path;
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

sub directory {
    my ($self, $path) = @_;
    my @dirs = grep { -d } $self->file($path);
    return @dirs if wantarray;
    die "multiple directories for $path" if @dirs > 1;
    return if !@dirs;
    return $dirs[0];
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

sub _read_map_files {
    my ($self) = @_;
    my %uuidmap;
    my %uuidunmap;
    # XXX Force failure to ferret out code that depended on this
    if (0) {
        my $uuid_maps_glob = $self->{'uuid-maps'} || 'map/*.uuidmap';
        my @files = $self->file($uuid_maps_glob);
        foreach my $file (@files) {
            (my $base = $file) =~ s{\.[^.]+$}{};
            $base =~ m{([^/]+)(?:\.[^/.]+)?$}
                or die "invalid UUID map file name: $file";
            my $kind = $1;
            open my $fh, '<', $file
                or die "open $file: $!";
            while (<$fh>) {
                next if /^\s*(?:#.*)?$/;  # Skip blank lines and comments
                chomp;
                if (/^(.+)=(.+)$/) {
                    my ($alias, $key) = map { trim($_) } ($1, $2);
                    my $val = $uuidmap{$kind}{$key};
                    $uuidmap{$kind}{$alias} = $val
                        or die "alias to undefined UUID in $file: $_";
                    $uuidunmap{$kind}{$val} ||= $alias;
                }
                elsif (/^([^:]+)(?:\s+:\s+(.+))?$/) {
                    my ($key, $val) = ($1, $2);
                    $val = $key if !defined $val;
                    $uuidmap{$kind}{$val} = $key;
                    $uuidunmap{$kind}{$key} ||= $val;
                }
            }
        }
    }
    $self->{'uuidmap'} = \%uuidmap;
    $self->{'uuidunmap'} = \%uuidunmap;
}

sub decode_uuid {
    my ($self, $kind, $uuid) = @_;
    return $self->{'uuidunmap'}{$kind}{$uuid};
}

sub encode_uuid {
    my ($self, $kind, $uuid, $obj) = @_;
    return $self->{'uuidunmap'}{$kind}{$uuid} = $obj;
}

sub expand_uuid {
    my ($self, $kind, $uuid, $prop) = @_;
    my $expanded = $self->{'uuidunmap'}{$kind}{$uuid};
    return "$uuid <$expanded>" if defined $expanded;
    my $obj = $self->object($kind, $uuid)
        or return $uuid;
    $prop ||= eval { $obj->_code_field };
    return sprintf '%s <%s>', $uuid, $obj->{$prop} // '' if defined $prop;
    foreach my $method (qw(_code code name desc description)) {
        my $sub = $obj->can($method)
            or next;
        my $val = $sub->($obj)
            // next;
        return "$uuid <$val>";
    }
    return "$uuid <$obj>";
}

sub _read_cache {
    my ($self) = @_;
    my %cache;
    # TODO
    $self->cache(\%cache);
}

sub state {
    my $self = shift;
    my $state_file = $self->file('var/state.json');
    if (@_ == 0) {
        my $state =  $self->{'state'};
        return $state if defined $state;
        return $self->{'state'} = _json_read($state_file);
    }
    elsif (@_ == 1) {
        _json_write($state_file, shift);
    }
}

sub local_db {
    my $self = shift;
    my $name = shift;
    my %arg = @_;
    my $file = $self->path("var/db/$name.db");
    return $self->{'local_db'}{$name} ||= Biblio::Folio::Site::LocalDB->new(
        'site' => $self,
        'name' => $name,
        'file' => $file,
        %arg,
    );
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

sub mapping_rules {
    my ($self) = @_;
    return $self->{'mapping_rules'} if $self->{'mapping_rules'};
    my $res = $self->GET('/mapping-rules');
    if ($res->is_success) {
        return $self->{'mapping_rules'} = $self->json->decode($res->content);
    }
    die "GET /mapping-rules: " . $res->status_line;
}

sub location {
    my $self = shift;
    return $self->object('location', @_);
}

sub all {
    my ($self, $kind) = @_;
    return $self->objects($kind, 'limit' => 1<<20);
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
    #
    # INSTANTIATING
    # -------------
    #   Search for all $kind objects:
    #       $searcher = $site->searcher($kind);
    #   Ditto but with a non-default batch size, offset, or both:
    #       $searcher = $site->searcher($kind, '@limit' => $limit);
    #       $searcher = $site->searcher($kind, '@offset' => $offset);
    #       $searcher = $site->searcher($kind, '@limit' => $limit, '@offset' => $offset);
    #   Search for $kind objects using search terms:
    #       $searcher = $site->searcher($kind, 'id' => [@ids], 'active' => true);
    #       $searcher = $site->searcher($kind, 'title' => [@titles], '@limit' => 20, '@offset' => 100);
    #   Search for $kind objects using a bespoke CQL query:
    #       $searcher = $site->searcher($kind, '@query' => $cql);
    #   Search for $kind objects using a file of identifiers:
    #       $searcher = $site->searcher($kind, '@file' => $file);
    #
    # SEARCHING AND RETRIEVING
    # ------------------------
    #   One record at a time:
    #       while (my $object = $searcher->next) { ... }
    #   Many records at a time:
    #       my @objects = $searcher->next(100);
    #
    die "unrecognized argument" if @_ % 2;
    my ($self, $kind, %term) = @_;
    my (%param, %arg, %obj, %cls2arg);
    my %atsy = (
        'query'      => \%param,
        'offset'     => \%param,
        'limit'      => \%param,
        'uri'        => \%arg,
        'file'       => ['ByIdFile'],
        'set'        => ['ByIdSet'],
        'id_field'   => \%arg,  # XXX ['ByIdFile', 'ByIdSet'],
        'batch_size' => \%arg,  # XXX ['ByIdFile', 'ByIdSet'],
    );
    # Look for things that aren't search terms
    foreach my $k (keys %term) {
        next if $k !~ /^\@(.+)/;
        my $v = delete $term{$k};
        $k = $1;
        my $what = $atsy{$k} or die "unrecognized searcher parameter: $k";
        my $r = ref $what;
        if ($r eq 'HASH') {
            $what->{$k} = $v;
        }
        elsif ($r eq 'ARRAY') {
            $cls2arg{$_}{$k} = $v for @$what;
        }
    }
    my $query = $param{'query'};
    my ($cls, @oops) = keys %cls2arg;
    if (defined $cls) {
        die "internal error: conflicting searcher classes: $cls @oops" if @oops;
        die "internal error: searcher class builds its own queries: $cls"
            if keys %term || defined $query;
        while (my ($k, $v) = each %{ $cls2arg{$cls} }) {
            $arg{$k} = $v;
        }
        $cls = 'Biblio::Folio::Site::Searcher::' . $cls;
    }
    else {
        $cls = 'Biblio::Folio::Site::Searcher';
        $arg{'terms'} = \%term if keys %term;
    }
    _use_class($cls);
    return $cls->new(
        'site' => $self,
        'kind' => $kind,
        'params' => \%param,
        %arg,
    );
}

sub harvester {
    my ($self, $name, %arg) = @_;
    my $pkg = __PACKAGE__ . '::Harvester::' . ucfirst _camel($name);
    _use_class($pkg);
    return $pkg->new(
        'site' => $self,
        %arg,
    );
}

sub local_instances_database {
    my ($self, $file) = @_;
    $file = 'var/db/instances.db'
        if !defined $file;
    $file = $self->file($file);
    return $self->{'local_instances_db'}{$file} ||= Biblio::Folio::Site::LocalDB::Instances->new(
        'file' => $file,
        'site' => $self,
    );
### my ($self) = @_;
### return $self->local_db('instances');
}

sub object {
    # $site->object($kind, $id);
    # $site->object($kind, $object_data);
    # $site->object($kind, \@ids);
    # $site->object($kind, 'terms' => \%term);
    # $site->object($kind, 'query' => $cql, 'limit' => $n, 'offset' => $p);
    # $site->object($kind, 'id' => $id, 'uri' => $uri);
    my ($self, $kind) = splice @_, 0, 2;
    my $pkg = _kind2pkg($kind);
    my (@args, @from, $id, $terms, $query);
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
    my ($uri, $objs, $id, $terms, $query) = @arg{qw(uri objects id terms query)};
    my $idref = ref $id;
    if (defined $terms) {
        die "fetch('$pkg', 'terms' => {...}, 'query' => q{$query}" 
            if defined $query;
        $query = _cql_query($terms);
    }
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
        }
        elsif (defined $id) {
            $uri = sprintf($uri, $id);
            $res = $self->GET($uri);
        }
        else {
            foreach (qw(offset limit)) {
                $content{$_} = $arg{$_} if defined $arg{$_};
            }
            delete $content{'query'};
            $uri =~ s{/%s$}{};  # or die "I don't know how to fetch a $pkg using URI $uri without a query or an ID";
            $res = $self->GET($uri, \%content);
        }
        $code = $res->code;
        return if $code eq '404';  # Not Found
        if ($res->is_success) {
            my $content = $self->json->decode($res->content);
            my @dig = grep { defined } (
                $arg{'dig'} ? @{ $arg{'dig'} }
                            : $arg{'key'} ? ($arg{'key'}) : ()
            );
            if (defined $id) {
                @return = ($content);
            }
            else {
                @return = $pkg->_search_results($content, @dig);
            }
        }
        else {
            die $res->status_line, ' : ', $uri;
        }
    }
    elsif (!@return) {
        die "can't construct or fetch $pkg objects without data or an ID or query";
    }
    my $instantiate = !$arg{'scalar'} && !$arg{'array'};
    return if !@return;
    @return = map {
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
        _json_decode($res->content);
    };
    if (!$content) {
        return if $res->code ne '201';
        $content = \%arg;
    }
    return $pkg->new('_site' => $self, '_json' => $json, %$content);
}

sub objects {
    my ($self, $kind, %arg) = @_;
    my $pkg = _kind2pkg($kind);
    my $uri = $pkg->_uri_search || $pkg->_uri;
    my $key = delete $arg{'key'};
    my @dig = defined $key ? ($key) : ();
    my $res = $self->GET($uri, \%arg);
    return if !$res->is_success;
    my $content = $self->json->decode($res->content);
    my @elems = $pkg->_search_results($content, @dig);
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
    my ($self, $method, $what, $content, $content_type, $accept) = @_;
    my $endpoint = $self->config('endpoint');
    my $state = $self->state;
    my $r = ref $what;
    my ($path, $uri, $req);
    if ($r eq '') {
        $uri = URI->new($endpoint->{'uri'} . $what);
        if ($content && keys %$content && ($method eq 'GET' || $method eq 'DELETE')) {
            $uri->query_form(%$content);
            undef $content_type;
            undef $content;
        }
        $req = HTTP::Request->new($method, $uri);
    }
    else {
        die "attempt to request a $r"
            if !$what->can('uri');
        $req = $what;
        $req->method($method);
        $uri = $req->uri;
        $uri = URI->new($uri) if !ref $uri;
        $path = $uri->path;
    }
    # Set the Content-Type: header and the body of the request
    if (defined $content_type && defined $content) {
        $content = $self->json->encode($content)
            if $content_type eq 'application/json';
        $req->content_type($content_type);
        $req->content($content);
    }
    elsif (!defined $content) {
        $req->content_type($content_type) if defined $content_type;
    }
    elsif ($method eq 'POST' || $method eq 'PUT') {
        $req->content_type('application/json');
        $req->content($self->json->encode($content));
    }
    elsif ($method ne 'GET') {
        die "content, but no content type? method: $method";
    }
    # Set the Accept: header
    if (defined $accept) {
        $req->header('Accept' => $accept);
    }
    elsif ($method eq 'GET' || $method eq 'POST') {
        $req->header('Accept' => 'application/json');
    }
    else {
        $req->header('Accept' => 'text/plain');
    }
    $req->header('X-Okapi-Tenant' => $endpoint->{'tenant'});
    # $req->header('X-Forwarded-For' => '69.43.75.60');
    if ($state->{'logged_in'}) {
        $req->header('X-Okapi-Token' => $state->{'token'});
    }
    if ($method eq 'POST' || $method eq 'PUT') {
#$req->header('Accept' => 'text/plain') if $method eq 'PUT';
        $req->content($self->json->encode($content));
    }
    return $req;
}

sub req {
    my $self = shift;
    my $req;
    if (@_ == 1) {
        ($req) = @_;
        die "not a request: $req" if !eval { $req->isa('HTTP::Request') };
    }
    else {
        my ($method, $what, $content) = @_;
        die "not a method: $method" if ref $method;
        $req = $self->make_request($method, $what, $content);
    }
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

sub source { goto &source_record }

sub source_record {
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
            my $res = $self->GET("/source-storage/records/$id/formatted", {'identifier' => 'INSTANCE'});
            return if !$res->is_success;
            $source = $self->json->decode($res->content);
        }
        elsif ($arg{'query'}) {
            die "Source record queries are no longer supported";
            my $query = $arg{'query'};
            my $res = $self->GET('/source-storage/source-records', {'query' => $query, 'limit' => 2});
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
        return _json_encode(\%instance);
    };
}

sub formatter {
    my ($self, $kind, %arg) = @_;
    my $pkg = _kind2pkg($kind);
    return $pkg->formatter(%arg);
}

sub class_for {
    my ($self, $kind, $actor, $profile) = @_;
    # $site->class_for('user_batch', 'sorter');
    # $site->class_for('user_batch', 'sorter', 'default');
    $profile = $self->profile($kind, $profile);
    my $pkg = $profile->{$actor}{'class'}
        or die "no class for $kind $actor: profile $profile->{'name'}";
    return $pkg;
}

sub sorter_for {
    my ($self, $kind, %arg) = @_;
    my $profile = $self->profile($kind, $arg{'profile'});
    my %sorter = %{ $profile->{'sorter'} || {} };
    my $sorter_cls = delete $sorter{'class'} || $self->class_for($kind, 'sorter', $profile);
    $sorter_cls = 'Biblio::FolioX::' . $sorter_cls if $sorter_cls =~ s/^[+](::)?//;
    _use_class($sorter_cls);
    return $sorter_cls->new(
        %sorter,
        'site' => $self,
        'profile' => $profile,
        'kind' => $kind,
    );
}

sub parser_for {
    my ($self, $kind, %arg) = @_;
    my $file = $arg{'file'};
    my $profile = $self->profile($kind, $arg{'profile'});
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
    my ($self, $kind, %arg) = @_;
    my $file = $arg{'file'};
    my $profile = $self->profile($kind, $arg{'profile'});
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
    my ($self, $kind, %arg) = @_;
    my $file = $arg{'file'};
    my $profile = $self->profile($kind, $arg{'profile'});
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

sub task {
    my ($self, $kind, %arg) = @_;
    my $pkg = _kind2pkg($kind, 'task');
    _use_class($pkg);
    my $now = time;
    my $ym = strftime('%Y-%m', localtime $now);
    my $id = sprintf '%s.%s.%s', _utc_datetime($now, 'compact'), $kind, _uuid();
    my @dirs = map { $self->path($_) } ('var/task', "var/task/running", "var/task/$ym", "var/task/running/$id", "var/task/$ym/$id", "var/task/$ym/$id/\@setup");
    my $root = $dirs[-2];
    _mkdirs(@dirs);
    pop @dirs;
    return $pkg->new(
        %arg,
        'root' => $root,
        'state' => 'setup',
        'site' => $self,
    );
}

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
