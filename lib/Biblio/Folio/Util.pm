package Biblio::Folio::Util;

use strict;
use warnings;

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    FORMAT_MARC
    FORMAT_JSON
    FORMAT_TEXT
    ENCODING_ASCII
    ENCODING_UTF8
    ENCODING_LATIN1
    ENCODING_CP1252
    _rx_const_token
    _tok2const
    _trim
    _camel
    _uncamel
    _2pkg
    _kind2pkg
    _pkg2kind
    _optional
    _cql_query
    _cql_value
    _cql_term
    _cql_and
    _cql_or
    _read_config
    _make_hooks
    _run_hooks
    _get_attribute_from_dotted
    _use_class
    _req
    _opt
    _str2hash
    _obj2hash
    _utc_datetime
    _uuid
    _bool
    _debug
    _cmpable
    _unbless
    _unique
    _int_set_str_to_hash
    _indentf
    _unindent
    _mkdirs
    _json_file
    _json_encode
    _json_decode
    _json_read
    _json_write
    _json_begin
    _json_append
    _json_end
    _detect_file_encoding
    _is_valid_utf8
    _force_valid_utf8
    _is_uuid
    _bool2int
    $rx_const_token
);

use constant FORMAT_MARC => 'marc';
use constant FORMAT_JSON => 'json';
use constant FORMAT_TEXT => 'text';

use constant ENCODING_ASCII => 'ascii';
use constant ENCODING_UTF8 => 'utf-8';
use constant ENCODING_LATIN1 => 'iso-8859-1';
use constant ENCODING_CP1252 => 'cp-1252';

use constant MICROSECONDS => '000000';

use JSON;
use Data::UUID;
use Data::Dumper;
use Scalar::Util qw(blessed);
use POSIX qw(strftime);
use File::Basename qw(dirname basename);

use constant DEBUGGING => $ENV{'DD'};

use constant FOLIO_UTC_FORMAT => '%Y-%m-%dT%H:%M:%S.%J+0000';
use constant COMPACT_UTC_FORMAT => '%Y%m%dT%H%M%SZ';

use constant TRUE => JSON::true;
use constant FALSE => JSON::false;
use constant NULL => JSON::null;

use constant CQL_TRUE => 'true';
use constant CQL_FALSE => 'false';
use constant CQL_NULL => 'null';

use vars qw($rx_const_token $rx_uuid);

my $json = JSON->new->pretty->canonical->convert_blessed->allow_blessed->allow_unknown;
my $dumper = Data::Dumper->new([])->Terse(1)->Indent(0)->Sortkeys(1)->Sparseseen(1);
my $uuidgen = Data::UUID->new;
my %tok2const = (
    CQL_TRUE() => TRUE,
    CQL_FALSE() => FALSE,
    CQL_NULL() => NULL,
);
$rx_const_token = qr/true|false|null/;

sub _tok2const {
    my ($tok) = @_;
    die "unrecognized const token: $tok"
        if !exists $tok2const{$tok};
    return $tok2const{$tok};
}

sub _read_config {
    my ($file, $config, $key) = @_;
    $config ||= {};
    $config = $config->{$key} ||= {} if defined $key;
    my $hash = $config;
    open my $fh, '<', $file or die "open $file: $!";
    while (<$fh>) {
        next if /^\s*(?:[#;].*)?$/;  # Skip blank lines and comments (# or ;)
        chomp;
        if (/^\s*\[(.+)\]\s*$/) {
            my $section = _trim($1);
            $section =~ s/\s+/-/g;
            $hash = $config->{$section} ||= {}
                if $section ne 'general';
        }
        else {
            my ($k, $v) = split /=/, $_, 2;
            die "config syntax: $_" if !defined $v;
            ($k, $v) = (_camel(_trim($k)), _trim($v));
            $k =~ s/\s+/-/g;
            $hash->{$k} = $v;
        }
    }
    return $config;
}

sub _make_hooks {
    # my %hooks = _make_hooks('foo' => \&foo, 'bar' => \&bar, 'foo' => \&other_foo);
    # my %hooks = _make_hooks('foo' => [\&foo, \&other_foo], 'bar' => \&bar, 'foo' => \&yet_other_foo);
    my %arg;
    while (@_ > 1) {
        my ($k, $v) = splice @_, 0, 2;
        my $oldv = $arg{$k};
        my $oldvref = ref $oldv;
        if (!defined $oldv) {
            $arg{$k} = $v;
        }
        elsif ($oldvref eq 'ARRAY') {
            if ($k eq 'before') {
                unshift @$oldv, $v;
            }
            else {
                push @$oldv, $v;
            }
        }
        elsif ($oldvref eq 'CODE') {
            if ($k eq 'before') {
                $arg{$k} = sub { $v->(@_); $oldv->(@_); };
            }
            else {
                $arg{$k} = sub { $oldv->(@_); $v->(@_); };
            }
        }
        else {
            die "can't apply $k action to a $oldvref";
        }
    }
    return %arg;
}

sub _run_hooks {
    my $phase = shift;
    my $hooks = shift or return;
    my $r = ref $hooks;
    $hooks = $hooks->{$phase} if $r eq 'HASH';
    if ($r eq 'CODE') {
        $hooks->('phase' => $phase, @_);
    }
    elsif ($r eq 'ARRAY') {
        $_->('phase' => $phase, @_) for @$hooks;
    }
    else {
        die "unrunnable hook: $r";
    }
}

sub _trim {
    local $_ = shift;
    s/^\s+|\s+$//g;
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

sub _2pkg {
    my ($x) = @_;
    return $x if $x =~ /^Biblio::Folio/;
    return _kind2pkg($x) if $x =~ /^[a-z_]+$/;
    return _kind2pkg(_pkg2kind($x));
}

sub _kind2pkg {
    my $kind = shift;
    my $base_kind = @_ ? shift : 'object';
    $base_kind ||= 'object';
    my @parts = qw(biblio folio);
    if ($base_kind eq 'object') {
        # _kind2pkg('foo_bar') => 'Biblio::Folio::Object::FooBar'
        # _kind2pkg('foo_bar', 'object') => 'Biblio::Folio::Object::FooBar'
        push @parts, 'object' if $kind ne 'object'; 
    }
    elsif ($base_kind eq 'site') {
        # _kind2pkg('task', 'site') => 'Biblio::Folio::Site::Task'
        push @parts, 'site' if $kind ne 'site';
    }
    else {
        # _kind2pkg('foo_bar', 'task') => 'Biblio::Folio::Site::Task::FooBar'
        push @parts, 'site', $base_kind;
    }
    return join('::', map { ucfirst _camel($_) } @parts, $kind); 
}

sub _old_kind2pkg {
    my ($kind, $base_kind) = @_;
    $base_kind = 'object' if !defined $base_kind;
    my $base_pkg = 'Biblio::Folio::' . ucfirst _camel($base_kind);
    return $base_pkg if $base_kind eq $kind;
    return $base_pkg . '::' . ucfirst _camel($kind);
}

sub _older_kind2pkg {
    my ($kind) = @_;
    return 'Biblio::Folio::Object' if $kind eq 'object';
    return 'Biblio::Folio::Object::' . ucfirst _camel($kind);
}

sub _pkg2kind {
    my ($pkg, $base_pkg) = @_;
    $base_pkg = 'Biblio::Folio::Object' if !defined $base_pkg;
    $pkg =~ s/^${base_pkg}:://;
    return lcfirst _uncamel($pkg);
}

sub _old_pkg2kind {
    my ($pkg) = @_;
    $pkg =~ s/^Biblio::Folio::Object:://;
    return lcfirst _uncamel($pkg);
}

sub _optional {
    my ($k, $v) = @_;
    return if !defined $v;
    return ($k, $v);
}

sub _cql_query {
    my ($terms) = @_;
    my @terms;
    while (my ($k, $v) = each %$terms) {
        next if $k =~ /^\@/;  # Query parameters (limit, offset)
        $k =~ s{^([~%]*)}{};
        my $signs = $1;
        my $exact  = ($signs =~ /~/ ? 0 : 1);
        my $is_cql = ($signs =~ /%/ ? 1 : 0);
        push @terms, _cql_term($k, $v, 'exact' => $exact, 'is_cql' => $is_cql);
    }
    return _cql_and(@terms);
}

sub _cql_value {
    my ($v, $is_cql) = @_;
    my $r = ref $v;
    return CQL_NULL if !defined $v;
    if ($r eq 'JSON::PP::Boolean') {
        # XXX Hard-coded
        return $v ? CQL_TRUE : CQL_FALSE;
    }
    elsif ($r eq 'ARRAY') {
        return _cql_or(map { _cql_value($_, $is_cql) } @$v);
    }
    elsif ($r ne '') {
        die "unrecognized value type in term: $r";
    }
    elsif ($is_cql) {
        return $v;
    }
    else {
        return $v if $v !~ s{(?=["\\?*^])}{\\}g  # Escape " \ ? * ^ (we don't grok wildcards)
                  && $v !~ m{[ =<>/()]};         # Special characters
        return qq{"$v"};
### $v =~ s/(["()\\\*\?])/\\$1/g;
    }
}

sub _cql_term {
    my ($k, $v, %arg) = @_;
    my $mp = $arg{'matchpoint'} || {};
    my $term = _cql_value($v, $arg{'is_cql'});
    return $term if !defined $k;
    my $exact = $arg{'exact'} // $mp->{'exact'};
    my $op = $exact ? '==' : '=';
    return $k . $op . $term;
}

sub _cql_and {
    return shift if @_ == 1;
    return '(' . join(' and ', @_) . ')';
}

sub _cql_or {
    return shift if @_ == 1;
    return '(' . join(' or ', @_) . ')';
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

sub _use_class {
    my ($cls) = @_;
    my $hash = eval qq{\\%${cls}::};
    return if keys %$hash;
    my $ok;
    eval qq{
        use $cls;
        \$ok = 1;
    };
    return if $ok;
    my ($err) = split /\n/, $@;
    die "use class $cls: $err";
}

sub _req {
    my ($k, $v) = @_;
    die "required field $k not present"
        if !defined $v;
    return ($k => $v);
}

sub _opt {
    my ($k, $v) = @_;
    return if !defined $v;
    return ($k => $v);
}

sub _str2hash {
    my ($str) = @_;
    my %hash = map {
        s/^\s+//;
        /^[^#]/ ? (split /\s+/, $_, 2) : ()
    } split /\n/, $str;
    return %hash if wantarray;
    return \%hash;
}

sub _unbless {
    my ($obj) = @_;
    my $ret;
    my (%hash, @array, $ok);
    eval { %hash = %$obj; $ok = 1 };
    return _as_hash(\%hash) if $ok;
    eval { @array = @$obj; $ok = 1 };
    return _as_array(\@array) if $ok;
    return $obj;
}

sub _as_hash {
    # Remove private elements (e.g., '_foo' => $bar)
    my ($hash) = @_;
    my %hash = %$hash;
    foreach my $k (keys %hash) {
        if ($k =~ /^_/) {
            delete $hash{$k};
            next;
        }
        my $v = $hash{$k};
        my $r = ref $v;
        if ($r eq 'ARRAY') {
            $hash{$k} = _as_array($v);
        }
        elsif ($r eq 'HASH') {
            $hash{$k} = _as_hash($v);
        }
    }
    return \%hash;
}

sub _as_array {
    my ($array) = @_;
    my @array = @$array;
    foreach my $i (0..$#array) {
        my $v = $array[$i];
        my $r = ref $v;
        if ($r eq 'ARRAY') {
            $array[$i] = _as_array($v);
        }
        elsif ($r eq 'HASH') {
            $array[$i] = _as_hash($v);
        }
        elsif (blessed($v)) {
            $array[$i] = _unbless($v);
        }
    }
    return \@array;
}

sub _utc_datetime {
    my ($t, $format) = @_;
    $format ||= FOLIO_UTC_FORMAT;
    $format = COMPACT_UTC_FORMAT if $format eq 'compact';
    return strftime($format, gmtime) if !defined $t;
    my ($fs);  # Fractions of a second are handled specially
    if ($t =~ m{
        ^
        ([0-9]+)
        (?:
            \.([0-9]+)
        )?
        $
    }x) {
        # Seconds since the Unix epoch
        ($t, $fs) = ($1, $2);
        local $ENV{'TZ'} = 'UTC';
        $t = strftime($format, gmtime $t);  # if $format !~ /^%s(?:\.%[36]Q)?$/;
    }
    else {
        # Ymd string with optional time and timezone
        $t =~ s/
            ^
            ([1-9][0-9][0-9][0-9])
            -?
            ([0-9][0-9])
            -?
            ([0-9][0-9])
        //x or die "malformed date: $t";
        my ($Y, $m, $d) = ($1, $2, $3);
        my ($H, $M, $S) = (0, 0, 0);
        my $z;
        if ($t =~ s/^T//) {
            if ($t =~ s/
                ^
                ([0-9][0-9])
                :?
                ([0-9][0-9])
                :?
                ([0-9][0-9])
                (?:
                    \.([0-9]+)
                )?
            //x) {
                ($H, $M, $S, $fs) = ($1, $2, $3, $4);
            }
            $z = $1 if $t =~ s/^(Z|[-+][0-9][0-9](?:[0-9][0-9])?)$//;
        }
        # die "junk at end of date/time: $t" if length $t;
        my @datetime = ($S, $M, $H, $d, $m-1, $Y-1900);
        if (!defined $z) {
            #local $ENV{'TZ'} = 'UTC';
            my $s = strftime('%s', @datetime);
            $t = strftime($format, gmtime $s);
        }
        elsif ($z =~ /^([-+]0000|Z)$/) {
            local $ENV{'TZ'} = 'UTC';
            my $s = strftime('%s', @datetime);
            $t = strftime($format, gmtime $s);
        }
        elsif ($z =~ /^([-+]) ([0-9][0-9]) (?:([0-9][0-9]))?$/x) {
            my $offset = $1 . int($2*3600 + ($3||0)*60);
            local $ENV{'TZ'} = 'UTC';
            $t = strftime('%s', @datetime) - $offset
#           local $ENV{'TZ'} = 'UTC'.$offset;
#           my $s = strftime('%s', @datetime);
#           # $s += $offset;  # ???
#           $t = strftime($format, gmtime $s);
        }
    }
    my ($ms, $us) = (0, 0);
    if (defined $fs) {
        my $fslen = length $fs;
        $fs .= substr(MICROSECONDS, -(6 - $fslen))
            if $fslen < 6;
        $ms = substr($fs, 0, 3);
        $us = substr($fs, 0, 6);
    }
    # Expand milliseconds (%J) and/or microseconds (%K)
    $t =~ s/%J/sprintf('%03d', $ms)/e;
    $t =~ s/%K/sprintf('%06d', $us)/e;
    return $t;
}

sub _uuid {
    return $uuidgen->create_str if !@_;
    my ($obj, $key) = @_;
    $key ||= 'id';
    return $obj->{$key}
        if defined $obj->{$key};
    return $obj->{$key} = $uuidgen->create_str;
}

sub _bool {
    $_[0] =~ /^[YyTt1]/ ? JSON::true : JSON::false
}

sub _bool2int {
    $_[0] ? 1 : 0
}

sub _debug {
    print STDERR "DEBUG @_\n" if DEBUGGING;
}

sub _cmpable {
    my ($v) = @_;
    return if !defined $v;
    my $r = ref $v;
    return $v if $r eq '';
    return $dumper->Dump([$v]);
### return join("\n", map { _cmpable($_) } @$v)
###     if $r eq 'ARRAY';
### return join("\n", map { $_ . "\t" . _cmpable($_) } sort keys %$v)
###     if $r eq 'HASH';
### return $v if !blessed($v);
### if ($v->can('(0+')) {
###     return $v + 0;
### }
### elsif ($v->can('bool')) {
###     return $v ? 1 : 0;
### }
### elsif ($v->can('""')) {
###     return "$v";
### }
### elsif ($v->can('as_string')) {
###     return $v->as_string;
### }
### else {
###     return $v;
### }
}

sub _unique {
    my %seen;
    my @unique;
    foreach (@_) {
        push @unique, $_ if !$seen{$_}++;
    }
    return @unique;
}

sub _int_set_str_to_hash {
    # 1-2,5 => ( 1 => 1, 2 => 1, 5 => 1 )
    my $str = shift;
    return if !defined $str || !length $str;
    my (%hash, $err);
    foreach (split /,/, $str) {
        if (/^([0-9]+)-([0-9]+)$/) {
            $err = 1 if $2 < $1;
            $hash{$_} = 1 for $1..$2;
        }
        elsif (/^([0-9]+)$/) {
            $hash{$1} = 1;
        }
        else {
            $err = 1;
        }
    }
    die "not a valid list of ranges: $str" if $err;
    return %hash;
}

sub _indentf {
    my ($lvl, $fmt) = splice @_, 0, 2;
    return scalar(' ' x $lvl) . sprintf($fmt, map { defined ? $_ : '' } @_);
}

sub _unindent {
    my @lines = map { split /(?<=\n)/ } @_;
    s/(?<!\n)\z/\n/, s/\A\t/    / for @lines;
    # N.B.: Each element of @lines now ends in "\n"
    my ($imax) = sort { $b <=> $a } map { m{^( +)} ? length($1) : 0 } @lines[0, -1];
    if ($imax != 0) {
        # First and last lines are not indented
        my $rx = "[ ]{1,$imax}";
        $rx = qr/$rx/;
        s/^$rx// for @lines;
    }
    return @lines if wantarray;
    return join('', @lines);
}

sub _move_to_dir {
    my $dest = pop;
    die "_move_to_dir(...,\$dest): not a directory: $dest"
        if !-d $dest;
    my ($ok, @moved) = (1);
    my %result = ('ok' => 1, 'moved' => [], 'errors' => {});
    foreach my $f (@_) {
        my $fnew = $dest . '/' . basename($f);
        if (rename($f, $fnew)) {
            push @moved, $fnew;
        }
        else {
            die "rename $f $fnew: $!" if !defined wantarray;
            $ok = $result{'ok'} = 0;
            $result{'errors'}{$f} = $!;
        }
    }
    return wantarray ? ($ok, @moved) : \%result;
}

sub _mkdirs {
    foreach my $d (@_) {
        -d $d or mkdir($d) or die "mkdir $d: $!";
    }
}

sub _json_file {
    my ($f) = @_;
    $f =~ s/(?:\.json)?$/.json/;
    return $f;
}

sub _json_encode {
    return $json->encode(@_);
}

sub _json_decode {
    return $json->decode(@_);
}

sub _json_read {
    my $f = _json_file(shift);
    open my $fh, '<', $f
        or die "open $f for reading: $!";
    local $/;
    my $str = <$fh>;
    close $fh or die "close $f: $!";
    my $content = $json->decode($str);
    return $content if defined $content;
    my ($default) = @_;
    die "no content in file: $f" if !defined $default;
    return $default;
}

sub _json_write {
    my $f = _json_file(shift);
    my $str = $json->encode(@_);
    open my $fh, '>', $f or die "open $f for writing: $!";
    print $fh $str;
    close $fh or die "close $f: $!";
}

sub _json_begin {
    my ($container, $k, $fh) = @_;
    my ($v, %context);
    $context{'fh'} = $fh ||= \*STDOUT;
    if (defined $k) {
        die "only hashes are supported by _json_begin, et al."
            if ref($container) ne 'HASH';
            # if !eval { defined((keys %$container, 1)[0]) };
        $v = delete $container->{$k};
        if (defined $v) {
            die "only arrays may be appended to by _json_append"
                if ref($v) ne 'ARRAY';
        }
        my $hstr = $json->encode($container);
        $hstr =~ s/\s*\}\s*\z//;
        print $fh $hstr;
        if (keys %$container) {
            print $fh ",\n";
        }
        else {
            print $fh "\n";
        }
        my $vstr = $json->encode({$k => []});
        $vstr =~ s/\s*\]\s*\}\s*\z//;
        $vstr =~ s/\A\{\s*//;
        print $fh '  ', $vstr;
        $context{'indent'} = '    ';
        $context{'end'} = "]\n}\n";
    }
    elsif (ref($container) eq 'ARRAY') {
        print $fh '[';
        $v = $container;
        $context{'indent'} = '  ';
        $context{'end'} = "]\n";
    }
    $context{'count'} = 0;
    _json_append(\%context, @$v) if defined $v && @$v;
    return \%context;
}

sub _json_append {
    my $ctx = shift;
    return if !@_;
    my $ind = $ctx->{'indent'};
    my $fh = $ctx->{'fh'};
    foreach (@_) {
        print $fh ',' if $ctx->{'count'}++;
        print $fh "\n", $ind;
        my $str = $json->encode($_);
        chomp $str;
        $str =~ s/^/$ind/msg;
        print $fh $str;
    }
}

sub _json_end {
    my $ctx = shift;
    my $fh = $ctx->{'fh'};
    print $fh "\n", $ctx->{'indent'} if $ctx->{'count'};
    print $fh delete $ctx->{'end'};
    my $f = $ctx->{'file'} || 'JSON output handle';
    close $fh or die "close $f: $!";
}

sub _detect_file_encoding {
    my ($file) = @_;
    my %possible = map { $_ => 1 } (ENCODING_ASCII, ENCODING_UTF8, ENCODING_LATIN1, ENCODING_CP1252);
    open my $fh, '<', $file or die "open $file: $!";
    binmode $fh or die "binmode $file: $!";
    while (<$fh>) {
        # N.B.: We ignore bytes \x00-\x1f -- their presence or absence is not a useful diagnostic
        next if /\A[\x00-\x7e]*\z/;
        delete $possible{ (ENCODING_ASCII) };
        if (/[\x7f-\x9f]/) {
            delete $possible{ (ENCODING_LATIN1) };
            delete $possible{ (ENCODING_CP1252) } if /[\x81\x8d\x8f\x90\x9d]/;
        }
        delete $possible{ (ENCODING_UTF8) } if !_is_valid_utf8($_);
        last if keys(%possible) < 2;
    }
    close $fh or die "close $file: $!";
    die "can't detect file encoding: $file" if !keys %possible;
    foreach (ENCODING_ASCII, ENCODING_UTF8, ENCODING_LATIN1) {
        return $_ if $possible{$_};
    }
    return ENCODING_CP1252;  # Egad!
}

sub _is_valid_utf8 {
    my ($str) = @_;
    return $str =~ m{
        \A
        (?: [\x00-\x7e]                           # ASCII
        | [\xc2-\xdf]         [\x80-\xbf]         # non-overlong 2-byte
        |  \xe0 [\xa0-\xbf]   [\x80-\xbf]         # excluding overlongs
        | [\xe1-\xec\xee\xef] [\x80-\xbf]{2}      # straight 3-byte
        |  \xed [\x80-\x9f]   [\x80-\xbf]         # excluding surrogates
        |  \xf0 [\x90-\xbf]   [\x80-\xbf]{2}      # planes 1-3
        | [\xf1-\xf3]         [\x80-\xbf]{3}      # planes 4-15
        |  \xf4 [\x80-\x8f]   [\x80-\xbf]{2}      # plane 16
        )*
        \z
    }x;
}

sub _force_valid_utf8 {
    my ($str) = @_;
    $str =~ s{
        (
              [\x00-\x7e]                             # ASCII
            | [\xc2-\xdf]         [\x80-\xbf]         # non-overlong 2-byte
            |  \xe0 [\xa0-\xbf]   [\x80-\xbf]         # excluding overlongs
            | [\xe1-\xec\xee\xef] [\x80-\xbf]{2}      # straight 3-byte
            |  \xed [\x80-\x9f]   [\x80-\xbf]         # excluding surrogates
            |  \xf0 [\x90-\xbf]   [\x80-\xbf]{2}      # planes 1-3
            | [\xf1-\xf3]         [\x80-\xbf]{3}      # planes 4-15
            |  \xf4 [\x80-\x8f]   [\x80-\xbf]{2}      # plane 16
        )
        |
        (.)
    }{
        defined $1 ? $1 : '?'
    }xeg;
    return $str;
}

sub _is_uuid {
    shift() =~ /^[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}$/;
}

1;
