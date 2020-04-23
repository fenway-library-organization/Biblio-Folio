package Biblio::Folio::Util;

use strict;
use warnings;

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
	_tok2const
	_trim
	_camel
	_uncamel
	_2pkg
	_kind2pkg
	_pkg2kind
	_optional
	_cql_value
	_cql_term
	_cql_and
	_cql_or
	_read_config
	_make_hooks
	_run_hooks
    _get_attribute_from_dotted
    _use_class
);

use JSON;

my %tok2const = (
    'null' => JSON::null,
    'true' => JSON::true,
    'false' => JSON::false,
);

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
    my %arg;
    my %salient = map { $_ => 1 } qw(begin before each after end);
    while (@_ > 1) {
        my ($k, $v) = splice @_, 0, 2;
        my $oldv = $arg{$k};
        my $oldvref = ref $oldv;
        if ($oldvref eq '' || !$salient{$k}) {
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
    my $hooks = shift
        or return;
    my $r = ref $hooks;
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
    my ($kind) = @_;
    return 'Biblio::Folio::Object' if $kind eq 'object';
    return 'Biblio::Folio::Object::' . ucfirst _camel($kind);
}

sub _pkg2kind {
    my ($pkg) = @_;
    $pkg =~ s/^Biblio::Folio::Object:://;
    return lcfirst _uncamel($pkg);
}

sub _optional {
    my ($k, $v) = @_;
    return if !defined $v;
    return ($k, $v);
}

sub _cql_value {
    my ($v, $is_cql) = @_;
    my $r = ref $v;
    if ($r eq 'JSON::PP::Boolean') {
        # XXX Hard-coded
        return 'true' if !!$v;
        return 'false';
    }
    elsif ($r eq 'ARRAY') {
        return _cql_or(map { _cql_value($_, $is_cql) } @$v);
    }
    elsif ($r ne '') {
        die "unrecognized value type in term: $r";
    }
    else {
        return $v if $v !~ s{(?=["\\?*^])}{\\}g  # Escape " \ ? * ^ (we don't grok wildcards)
                  && $v !~ m{[ =<>/()]};         # Special characters
        return qq{"$v"};
### $v =~ s/(["()\\\*\?])/\\$1/g;
    }
}

sub _cql_term {
    my ($k, $v, $mp, $is_cql) = @_;
    my $term = _cql_value($v, $is_cql);
    return $term if !defined $k;
    my $op = $mp->{'exact'} ? '==' : '=';
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
    my $ok;
    eval qq{
        use $cls;
        \$ok = 1;
    };
    return if $ok;
    my ($err) = split /\n/, $@;
    die "use class $cls: $err";
}

1;
