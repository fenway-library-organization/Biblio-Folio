package Biblio::Folio::Site;

use strict;
use warnings;

use JSON;
use Digest;
use DBI;
use Text::Balanced qw(extract_delimited);

use Biblio::Folio::Class;
use Biblio::Folio::Object;
use Biblio::Folio::Util qw(_read_config _2pkg _pkg2kind _kind2pkg _optional _use_class);
use Biblio::Folio::Site::Stash;
use Biblio::Folio::Site::LoadProfile;
use Biblio::Folio::Site::Matcher;

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
            }
            elsif ($uri !~ s{/%s$}{}) {
                die "I don't know how to fetch a $pkg using URI $uri without a query or an ID";
            }
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
    my $n = delete $content->{'totalRecords'};
    delete @$content{qw(resultInfo errorMessages)};
    if (!defined $key) {
        my @keys = keys %$content;
        die "which key?" if @keys != 1;
        ($key) = @keys;
    }
    my $objects = $content->{$key};
    die "not an array: $key" if !$objects || ref($objects) ne 'ARRAY';
    return map {
        $pkg->new('_site' => $self, '_json' => $self->json, %$_)
    } @$objects;
}

sub matcher {
    my ($self, $kind, %arg) = @_;
    my $p = delete $arg{'profile'};
    my $profile = $self->load_profile($kind, $p);
    return Biblio::Folio::Site::Matcher->new(
        'site' => $self,
        'kind' => $kind,
        'profile' => $profile,
        %arg,
    );
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
    my $self = shift;
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
    return Biblio::Folio::SourceRecord->new('_site' => $self, %$source);
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
    my %parser = %{ $profile->{'parser'} };
    my $parser_cls = $parser{'class'} || 'Biblio::FolioX::Util::JSONParser';
    $parser_cls = 'Biblio::FolioX' . $parser_cls if $parser_cls =~ /^[+]/;
    delete $parser{'class'};
    _use_class($parser_cls);
    return $parser_cls->new('site' => $self, %parser, 'file' => $file);
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
