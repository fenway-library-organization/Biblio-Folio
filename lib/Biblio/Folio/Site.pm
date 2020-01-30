package Biblio::Folio::Site;

use strict;
use warnings;

use JSON;
use Digest;
use DBI;
use Text::Balanced qw(extract_delimited);

use Biblio::Folio::Class;
use Biblio::Folio::Object;
use Biblio::Folio::Site::Stash;

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
sub config { @_ > 1 ? $_[0]{'config'} = $_[1] : $_[0]{'config'} }
sub ua { @_ > 1 ? $_[0]{'ua'} = $_[1] : $_[0]{'ua'} }
sub json { @_ > 1 ? $_[0]{'json'} = $_[1] : $_[0]{'json'} }

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
            ($k, $v) = (camel(trim($k)), trim($v));
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
    my $folio = $self->folio;
    $self->{'root'} = $folio->root . "/site/$name";
    $self->_initialize_classes_and_properties;
    $self->_read_config_files;
    $self->_read_map_files;
    $self->_read_cache;
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
    my %state = %{ $self->state };
    return $self if $state{'logged_in'} && $arg{'reuse_token'};
    my $config = $self->config;
    my $res = $self->POST('/authn/login', {
        'username' => $config->{'endpoint'}{'user'},
        'password' => $config->{'endpoint'}{'password'},
    }) or die "login failed";
    my $token = $res->header('X-Okapi-Token')
        or die "login didn't yield a token";
    my $content = $self->json->decode($res->content);
    my $user_id = $content->{'userId'};
    @state{qw(token logged_in user_id)} = ($token, 1, $user_id);
    $self->state(\%state);
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
    my $class = $self->{'_classes'}{$pkg};
    return $class if $class && $class->is_defined;
    my $kind = _pkg2kind($pkg);
    $class ||= Biblio::Folio::Class->new(
        'site' => $self,
        'package' => $pkg,
        'kind' => $kind,
        'ttl' => 1,
        'uri' => {},  # XXX
    );
    $class->define;
}

sub property {
    my ($self, $p) = @_;
    my $prop = $self->{'_properties'}{$p};
    return $prop if $prop;
    die "no such property: $prop";
}

sub object {
    my ($self, $kind, @args) = @_;
    my $pkg = _kind2pkg($kind);
    if (@args == 1) {
        my ($arg) = @args;
        if (ref $arg) {
            @args = %$arg;
        }
        else {
            @args = ('id' => $arg);
        }
    }
    my $class = $self->class($pkg);
    return $self->fetch($pkg->new('_site' => $self, '_json' => $self->json, @args));
}

sub fetch {
    my ($self, $obj) = @_;
    my $id = $obj->{'id'};
    my $res;
    if (defined $id) {
        my $uri = $obj->_uri('fetch');
        $res = eval { $self->GET(sprintf $uri, $id) };
    }
    else {
        die "not yet implemented";
    }
    return if !$res;
    %$obj = (%$obj, %{ $self->json->decode($res->content) });
    return $obj->init;
}

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
    my $res = eval { $self->GET($uri, \%arg) };
    return if !$res;
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
    my ($self, $method, $url, $content) = @_;
    my $config = $self->config;
    my $state = $self->state;
    my $uri = URI->new($config->{'endpoint'}{'uri'} . $url);
    if ($content && keys %$content && ($method eq 'GET' || $method eq 'DELETE')) {
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
        $req->content($self->json->encode($content));
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

sub camel {
    local $_ = shift;
    s/[_\s]+(.)/\U$1/g;
    return $_;
}

sub uncamel {
    local $_ = shift;
    s/(?<=[a-z])(?=[A-Z])/_/g;
    return lc $_;
}

sub _apply_update_to_user {
    my ($self, $existing, $incoming) = @_;
    # Apply changes from $incoming to $existing
    my @changes;
    my $updating = $self->config->{'users'}{'update'};
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
    my $res;
    if (@_ == 1) {
        my $id = shift;
        return $self->object('source' => $id);
        #$res = eval { $self->GET("/source-storage/records/$id", { @_ }) }
        #    or return;
        #$res = $self->json->decode($res->content);
    }
    else {
        my %arg = @_;
        if ($arg{'instance'}) {
            my $id = $arg{'instance'};
            $res = eval { $self->GET('/source-storage/formattedRecords/'.$id, {'identifier' => 'INSTANCE'}) }
                or return;
            $res = $self->json->decode($res->content);
        }
        elsif ($arg{'query'}) {
            my $query = $arg{'query'};
            $res = eval { $self->GET('/source-storage/records', {'query' => $query, 'limit' => 2}) }
                or return;
            $res = $res->{'records'}
                or return;
            return if ref($res) ne 'ARRAY' || @$res != 1;
            $res = $res->[0];
        }
        else {
            die '$site->source($id|instance=>$id|query=>$cql)';
        }
    }
    return Biblio::Folio::SourceRecord->new('_site' => $self, %$res);
}

sub _initialize_classes_and_properties {
    my ($self) = @_;
    my (%class, %prop2class, %unresolved);
    # Property name (without Id/Ids)    Definition
    # Key to property definitions:
    #   =PROP   same as PROP (must be the only flag)
    #   +       cached (default TTL)
    #   NUM     cached TTL
    #   auto    auto-instantiated
    #   :CLASS  class to use
    my $file = $self->
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
            my %p;
            my $kind;
            if ($name =~ m{(.+)Ids?$}) {
                $kind = $p{'kind'} = uncamel($1);
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
                elsif (s{^(?:fetch:)?(/\S+)}{}) {
                    (my $uri = $1) =~ s/{[^{}]+}/%s/;
                    $p{'uri'}{'fetch'} = $uri;
                }
                elsif (s{^uri:(/\S+)}{}) {
                    $p{'uri'}{'base'} = $1;
                }
                elsif (s{^search:(/\S+)}{}) {
                    $p{'uri'}{'search'} = $1;
                }
                elsif (s/^(UUID|LITERAL)//) {
                    $p{'type'} = $1;
                }
                else {
                    die $err;
                }
            }
            if (defined $kind && $p{'type'} ne LITERAL) {
                my $pkg = 'Biblio::Folio::' . ucfirst camel($kind);
                $p{'package'} = $pkg;
                $class{$pkg} = \%p;
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
            if (exists $property{$name}) {
                $property{$alias} = $property{$name};
                delete $unresolved{$alias};
            }
        }
    }
    my @unresolved = sort keys %unresolved;
    die "unresolved property aliases: @unresolved" if @unresolved;
    $self->{'_properties'} = \%property;
    my $blessings = q{
        Metadata                *.metadata
        Classification          Instance.classifications[]
        Contributor             Instance.contributors[]
        InstanceNote            Instance.notes[]
        Identifier              Instance.identifiers[]
    };
    foreach my $blessing (split /\n/, $blessings) {
        next if $blessing =~ /^\s*(?:#.*)?$/;  # Skip blank lines and comments
        chomp $blessing;
        $blessing =~ s/^\s+//;
        $blessing =~ s/\s+/ /g;
        my $err = "internal error: unrecognized blessing: $blessing";
        $blessing =~ s/^([A-Z][A-Za-z]*) // or die $err;
        my $pkg = 'Biblio::Folio::' . $1;
        my @from = split /, /, $blessing;
        foreach (@from) {
            /^(\*|[A-Z][A-Za-z]*)\.([a-z][A-Za-z]*)(\[\])?$/ or die $err;
            my ($from_cls, $from_property, $each) = ($1, $2, defined $3);
            $from_cls = 'Biblio::Folio::' . $from_cls if $from_cls ne '*';
            $class{$from_cls} ||= {
                'site' => $site,
                'kind' => uncamel($from_cls),
                'blessings' => [],
                'ttl' => 1,
                'uri' => {},  # XXX
            };
            push @{ $class{$from_cls}{'blessings'} }, {
                'property' => $from_property,
                'package' => $pkg,
                'each' => $each,
            };
        }
    }
    $self->{'_classes'} = \%class;
    $self->_define_classes(values %class);
}

sub _quote {
    my $str = shift;
    return 'undef' if !defined $str;
    die if ref $str;
    return qq{"\Q$str\E"};
}

sub _kind2pkg {
    my ($kind) = @_;
    return 'Biblio::Folio::' . ucfirst camel($kind);
}

sub _pkg2kind {
    my ($pkg) = @_;
    $pkg =~ s/^Biblio::Folio:://;
    return lcfirst uncamel($pkg);
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

sub AUTOLOAD {
    # $site->user($user_id) --> $site->object('user', $user_id);
    # $site->campus('query' => 'name = "Riverside Campus"');
    my $self = shift;
    die if !@_;
    (my $kind = $AUTOLOAD) =~ s/.*:://;
    my $pkg = ucfirst camel($kind);
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
