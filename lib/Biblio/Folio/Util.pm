package Biblio::Folio::Util;

sub read_config {
    my ($file, $config, $key) = @_;
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
    return 'Biblio::Folio::' . ucfirst _camel($kind);
}

sub _pkg2kind {
    my ($pkg) = @_;
    $pkg =~ s/^Biblio::Folio:://;
    return lcfirst _uncamel($pkg);
}

sub _optional {
    my ($k, $v) = @_;
    return if !defined $v;
    return ($k, $v);
}

1;
