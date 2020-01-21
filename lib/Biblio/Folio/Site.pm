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
my (%class, %property);
initialize_classes_and_properties();

sub new {
    my $cls = shift;
    unshift @_, 'name' if @_ % 2;
    my $self = bless { @_ }, $cls;
    return $self->init;
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
    my $folio = $self->{'folio'};
    $self->{'root'} = $folio->root . "/site/$name";
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
    $self->{'cached_object'} = \%cache;
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
    return $self if $state{'logged_in'} && $arg{'reuse_token'};
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
    my $cached = $self->{'cached_object'}{$key};
    my $t = time;
    return $cached->{'object'} if $cached && $cached->{'expiry'} >= $t;
    my $obj = eval { $self->object($kind, $id) };
    my $ttl = $obj->ttl || 3600;  # One hour
    if ($ttl != -1) {
        $self->{'cached_object'}{$key} = {
            'object' => $obj,
            'expiry' => $t + $ttl,
        };
    }
    return $obj;
}

sub object {
    my ($self, $kind, @args) = @_;
    my $cls = 'Biblio::Folio::' . ucfirst camel($kind);
    if (@args == 1) {
        my ($arg) = @args;
        if (ref $arg) {
            @args = %$arg;
        }
        else {
            @args = ('id' => $arg);
        }
    }
    if (!eval "keys %${cls}::") {
        define_class($cls);
    }
    return $cls->new('_site' => $self, @args)->fetch;
}
    
sub define_class {
    my ($cls) = @_;
    my $ttl = $class{$cls}{'ttl'} || 1;
    my $uri = $class{$cls}{'uri'} || die;
    my $pkg_code = qq{
        package $cls;
        \@${cls}::ISA = qw(Biblio::Folio::Object);
        sub ttl { $ttl }
        sub _obj_uri { q{$uri} }
    };
    # TODO
}

sub objects {
    my ($self, $uri, %arg) = @_;
    my $key = delete $arg{'key'};
    my $res = eval { $self->get($uri, \%arg) };
    return if !$res;
    my $content = $json->decode($res->content);
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

sub source {
    my $self = shift;
    my $res;
    if (@_ == 1) {
        my $id = shift;
        return $self->object('source' => $id);
        #$res = eval { $self->get("/source-storage/records/$id", { @_ }) }
        #    or return;
        #$res = $json->decode($res->content);
    }
    else {
        my %arg = @_;
        if ($arg{'instance'}) {
            my $id = $arg{'instance'};
            $res = eval { $self->get('/source-storage/formattedRecords/'.$id, {'identifier' => 'INSTANCE'}) }
                or return;
            $res = $json->decode($res->content);
        }
        elsif ($arg{'query'}) {
            my $query = $arg{'query'};
            $res = eval { $self->get('/source-storage/records', {'query' => $query, 'limit' => 2}) }
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

sub initialize_classes_and_properties {
    my %unresolved;
    my %class;
    # Property name (without Id/Ids)    Definition
    # Key to property definitions:
    #   =PROP   same as PROP (must be the only flag)
    #   +       cached (default TTL)
    #   NUM     cached TTL
    #   auto    auto-instantiated
    #   :CLASS  class to use
    my $properties = q{
        addressTypeId                       +3600   /addresstypes/{addresstypeId}
        alternativeTitleTypeId              +3600   /alternative-title-types/{id}
        callNumberTypeId                    +3600   /call-number-types/{id}
        campusId                            +       /location-units/campuses/{id}
        classificationTypeId                +3600   /classification-types/{classificationTypeId}
        contributorNameTypeId               +3600   /contributor-name-types/{contributorNameTypeId}
        contributorTypeId                   +3600   /contributor-types/{contributorTypeId}
        copyrightStatusId                   +       /coursereserves/copyrightstatuses/{status_id}
    ### countryId -- not a dereferenceable identifier
        courseId                            +       /coursereserves/courses/{course_id}
        courseListingId                     +       /coursereserves/courselistings/{listing_id}
        courseTypeId                        +       /coursereserves/coursetypes/{type_id}
        defaultServicePointId               =servicePointId
        departmentId                        +       /coursereserves/departments/{department_id}
        effectiveLocationId                 =locationId
    ### externalId -- not a dereferenceable identifier
    ### externalSystemId -- not a dereferenceable identifier
        formerId                            +
        holdingsNoteTypeId                  +
        holdingsRecordId                    1
        holdingsTypeId                      +
        identifierTypeId                    +
        illPolicyId                         +
        inTransitDestinationServicePointId  =servicePointId
        instanceId                          1
        instanceFormatId                    +
        instanceNoteTypeId                  +
        instanceRelationshipTypeId          +
        instanceTypeId                      +
        institutionId                       +       /location-units/campuses/{id}
        intervalId                          +
        itemId                              +       /item-storage/items/%s
        itemDamagedStatusId                 +
        itemLevelCallNumberTypeId           =callNumberTypeId
        itemNoteTypeId                      +
        libraryId                           +       /location-units/campuses/{id}
        locationId                          +
        materialTypeId                      +
        modeOfIssuanceId                    +
        natureOfContentTermId               +
        permanentLoanTypeId                 +
        permanentLocationId                 +
        platformId                          +
        preferredContactTypeId              +
        processingStatusId                  +
        proxyUserId                         +
        registerId                          +
        registrarId                         +
        relationshipId                      +
        scheduleId                          +
        servicePointId                      +
        servicePointsId                     =servicePointId
        servicepointId                      =servicePointId
        sourceRecordId                      +
        staffMemberId                       +
        statisticalCodeId                   +
        statisticalCodeTypeId               +
        statusId                            +
        subInstanceId                       +
        superInstanceId                     +
        temporaryLoanTypeId                 +
        temporaryLocationId                 +
        termId                              +
        typeId                              +
        userId                              +
    };
    foreach (split /\n/, $properties) {
        next if /^\s*(?:#.*)?$/;  # Skip blank lines and comments
        s/^\s*(\S+)// or die "wtf?";
        my $name = $1;
        my @names = $name =~ m{Id/s$} ? ($name, $name.'s') : ($name);
        foreach $name (@names) {
            $unresolved{$name} = $1, next if /^\s+=\s*(\w+)/;
            my %p = ('name' => $name);
            my $kind;
            if ($name =~ m{(.+)Ids?$}) {
                $p{'kind'} = uncamel($1);
            }
            while (s/^\s+(?=\S)//) {
                if (s/^[+]([0-9]*|(?=\s)|$)//) {
                    $p{'ttl'} = length $1 ? $1 : 1;
                }
                elsif (s/^-(?:(?=\s)|$)//) {
                    $p{'ttl'} = 0;
                }
                elsif (s/^!(\S+)//) {
                    $p{'method'} = $1;
                }
                elsif (s/^%(\S+)//) {
                    $kind = $p{'kind'} = $1;
                }
                elsif (s{^(/\S+)}{}) {
                    $p{'uri'} = $1;
                }
            }
            if (defined $kind) {
                $p{'class'} = 'Biblio::Folio::' . ucfirst camel($kind);
            }
            $property{$name} = \%p;
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
}

package Biblio::Folio::Object;

@Biblio::Folio::Instance::ISA =
@Biblio::Folio::HoldingsRecord::ISA =
@Biblio::Folio::Item::ISA =
@Biblio::Folio::SourceRecord::ISA =
@Biblio::Folio::Location::ISA =
@Biblio::Folio::CallNumberType::ISA =
    qw(Biblio::Folio::Object);

*camel = *Biblio::Folio::Site::camel;

our $AUTOLOAD;

sub ttl { 3600 }

sub new {
    my $cls = shift;
    my $self = bless { @_ }, $cls;
    return $self->init;
}

sub DESTROY { }

sub init {
    my ($self) = @_;
    return $self;
    # XXX
    my $site = $self->site;
    my @auto_deref = eval { $self->_auto_deref };
    foreach my $method (@auto_deref) {
        $self->$method;
        next;
# Old code:
###     $prop =~ /^([-+]?)(.+)Id(s?)$/;
###     my ($nocache, $newprop, $plural) = ($1, $2, $3);
###     $newprop .= $plural;
###     my $sub = $nocache ? $site->can('object') : $site->can('cached');
###     if ($plural) {
###         $self->{$newprop} = [ map { $sub->($site, $kind, $_) } @{ $self->{$prop} } ];
###     }
###     else {
###         $self->{$newprop} = $sub->($site, $kind, $_);
###     }
    }
    return $self;
}

sub cached {
    unshift @_, shift(@_)->{'_site'};
    goto &Biblio::Folio::Site::cached;
}

sub site {
    return $_[0]{'_site'} = $_[1] if @_ > 1;
    return $_[0]{'_site'};
}

sub fetch {
    my ($self) = @_;
    my $site = $self->site;
    my $id = $self->{'id'};
    my $res;
    if (defined $id) {
        my $uri = $self->_obj_uri;
        $res = eval {
            $self->site->get(sprintf $uri, $id);
        };
    }
    else {
        die "not yet implemented";
    }
    return if !$res;
    %$self = (%$self, %{ $json->decode($res->content) });
    return $self;
}

sub AUTOLOAD {
    die if @_ > 1;
    my ($self) = @_;
    (my $called_as = $AUTOLOAD) =~ s/.*:://;
    # NOTE:
    #   ($key, $val) = (key under which the returned value is stored, the returned value)
    #       ('title', '...')
    #       ('callNumberType', { ... })
    #   ($rkey, $rval) = (reference key, reference value)
    #       ('callNumberTypeId', '84f4e01c-41fd-44e6-b0f1-a76330a56bed')
    my $key = camel($called_as);
    my $val = $self->{$key};
    my $rkey;
    if (exists $self->{$key.'Id'}) {
        $rkey = $key.'Id';
    }
    elsif ($key =~ /^(.+)s$/ && exists $self->{$1.'Ids'}) {
        ($key, $rkey) = ($1, $1.'Ids');
    }
    my $prop = $property{$key};
    if (!defined $rkey || !defined $prop) {
        # No dereferencing is possible
        no strict 'refs';
        *$AUTOLOAD = sub {
            my ($self) = @_;
            return $self->{$key};
        };
        return $val;
    }
    my $rval = $self->{$rkey};
    my $kind = $prop->{'kind'};
    my $ttl  = $prop->{'ttl'};
    my $get_method = $self->can($prop->{'method'} || ($ttl ? 'cached' : 'object'));
    if ($rkey =~ /Ids$/) {
        no strict 'refs';
        *$AUTOLOAD = sub {
            my ($self) = @_;
            my @vals = map { $get_method->($self, $kind, $self->{$key.'Id'}) } @$rval;
            $val = $self->{$key} = \@vals;
            return wantarray ? @vals : $val;
        };
    }
    else {
        no strict 'refs';
        *$AUTOLOAD = sub {
            my ($self) = @_;
            return $self->{$key} = $get_method->($self, $kind, $self->{$key.'Ids'});
        }
    }
    return if !defined $rval;  # NULL reference
    goto &$AUTOLOAD;
}

package Biblio::Folio::Instance;

sub ttl { 1 }

sub _obj_uri { '/instance-storage/instances/%s' }

sub holdings {
    my ($self, $id_or_query) = @_;
    my $site = $self->site;
    my $holdings;
    if (!defined $id_or_query) {
        return @{ $self->{'holdings'} }
            if $self->{'holdings'};
        my $id = $self->{'id'};
        $holdings = $site->objects('/holdings-storage/holdings', 'query' => "instanceId==$id");
    }
    elsif (!ref $id_or_query) {
        $holdings = $site->objects('/holdings-storage/holdings', 'query' => $id_or_query);
    }
    else {
        $holdings = $site->objects('/holdings-storage/holdings', %$id_or_query);
    }
    return if !$holdings;
    $self->{'holdings'} = $holdings;
    return map { Biblio::Folio::HoldingsRecord->new('_site' => $site, 'instance' => $self, %$_) } @$holdings;
}

package Biblio::Folio::HoldingsRecord;

sub ttl { 1 }

sub _obj_uri { '/holdings-storage/holdings/%s' }

sub call_number { shift()->{'callNumber'} }

sub items {
    my ($self, $id_or_query) = @_;
    my $site = $self->site;
    my $items;
    if (!defined $id_or_query) {
        return @{ $self->{'items'} }
            if $self->{'items'};
        my $id = $self->{'id'};
        $items = $site->objects('/item-storage/items', 'query' => "holdingsRecordId==$id");
    }
    elsif (!ref $id_or_query) {
        $items = $site->objects('/item-storage/items', 'query' => $id_or_query);
    }
    else {
        $items = $site->objects('/item-storage/items', %$id_or_query);
    }
    return if !$items;
    $self->{'items'} = $items;
    return map { Biblio::Folio::Item->new('_site' => $site, 'holdingsRecord' => $self, %$_) } @$items;
}

sub permanent_location {
    my ($self) = @_;
    return $self->{'permanentLocation'} = $self->cached('location' => $self->{'permanentLocationId'});
}

sub effective_location {
    my ($self) = @_;
    return $self->{'effectiveLocation'} = $self->cached('location' => $self->{'effectiveLocationId'});
}

sub call_number_type {
    my ($self) = @_;
    return $self->{'callNumberType'} = $self->cached('call_number_type' => $self->{'callNumberTypeId'});
}

package Biblio::Folio::SourceRecord;

sub ttl { 1 }

sub as_marc {
    my ($self) = @_;
    return $self->{'rawRecord'}{'content'};
}

package Biblio::Folio::Item;

sub ttl { 1 }

sub _obj_uri { '/item-storage/items/%s' }

sub location {
    my ($self) = @_;
    return $self->cached('location' => $self->{'effectiveLocationId'});
}

sub Biblio::Folio::Location::_obj_uri       { '/locations/%s' }
sub Biblio::Folio::CallNumberType::_obj_uri { '/call-number-types/%s' }
sub Biblio::Folio::LoanType::_obj_uri       { '/loan-types/%s' }
sub Biblio::Folio::Institution::_obj_uri    { '/location-units/institutions/%s' }
sub Biblio::Folio::Campus::_obj_uri         { '/location-units/campuses/%s' }
sub Biblio::Folio::Library::_obj_uri        { '/location-units/libraries/%s' }

1;

