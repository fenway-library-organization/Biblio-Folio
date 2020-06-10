package Biblio::Folio::Object;

use strict;
use warnings;

use constant qw(LITERAL LITERAL);
use constant qw(UUID    UUID   );

use Biblio::Folio::Util qw(_camel _unbless);

our $AUTOLOAD;

# sub ttl { 3600 }

sub new {
    my $cls = shift;
    my $self = bless { @_ }, $cls;
    $self->init;
    return $self;
}

sub DESTROY { }

sub init {
    my $self = shift;
    return $self;
}

sub _uri_create {
    my ($self, $obj) = @_;
    $self = bless { %$obj }, $self if !ref $self;
    my $uri = $self->_uri;
    $uri =~ s{/%s$}{};
    return $uri;
}

sub _uri_update {
    my ($self, $obj) = @_;
    $self = bless { %$obj }, $self if !ref $self;
    my $uri = $self->_uri;
    my $id = $self->id;
    $uri =~ s{/%s$}{/$id}
        or die "no placeholder in uri $uri to use for updates";
    return $uri;
}

sub _uri_delete {
    my ($self, $obj) = @_;
    $self = bless { %$obj }, $self if !ref $self;
    my $uri = $self->_uri;
    my $id = $self->id;
    $uri =~ s{/%s$}{/$id}
        or die "no placeholder in uri $uri to use for deletes";
    return $uri;
}

sub _search_results {
    my ($pkg, $content, @dig) = @_;
    return if !defined $content;
    my $cref = ref $content;
    if ($cref eq 'HASH') {
        if (@dig) {
            my @dug;
            while (@dig) {
                return if !defined $content;
                my $k = shift @dig;
                push @dug, $k;
                if ($cref eq 'ARRAY') {
                    $k =~ /^-?[0-9]+$/
                        or die "non-numeric key into array: ", join('.', @dug, $k);
                    $content = $content->[$k];
                }
                elsif ($cref eq 'HASH') {
                    $content = $content->{$k};
                }
                elsif ($cref eq '') {
                    $content = eval { $pkg->$k($content) };
                }
                $cref = ref $content;
            }
        }
        else {
            my ($info, $total) = delete @$content{qw(resultInfo totalRecords)};
            if (defined $total) {
                my @data;
                foreach (keys %$content) {
                    my $array = $content->{$_};
                    if (ref($array) eq 'ARRAY') {
                        push @data, $array;
                    }
                }
                if (@data == 1) {
                    ($content) = @data;
                    $cref = ref $content;
                }
                elsif (@data > 1) {
                    die "can't determine how to get $pkg search results: multiple keys = ", join(', ', @data);
                }
            }
        }
    }
    if (wantarray && $cref eq 'ARRAY') {
        return @$content;
    }
    else {
        return $content;
    }
}

sub TO_JSON {
    return _unbless(shift());
    #my %self = %{ shift() };
    #delete @self{grep { /^_/ || ref($self{$_}) !~ /^(?:ARRAY|HASH)?$/ } keys %self};
    #return \%self;
}

sub cached {
    unshift @_, shift(@_)->{'_site'};
    goto &Biblio::Folio::Site::cached;
}

sub site {
    return $_[0]{'_site'} = $_[1] if @_ > 1;
    return $_[0]{'_site'};
}

sub properties {
    my $self = shift;
    return $self->site->properties($self, @_);
}

sub property {
    my ($self, $name) = @_;
    return $self->$name;
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
    my $site = $self->site;
    my $key = _camel($called_as);
    my $val = $self->{$key};
    my ($prop, $rkey);
    if (exists $self->{$key.'Id'}) {
        $rkey = $key.'Id';
        $prop = $site->property($rkey);
    }
    elsif ($key =~ /^(.+)s$/ && exists $self->{$1.'Ids'}) {
        ($key, $rkey) = ($1, $1.'Ids');
        $prop = $site->property($key);
    }
    if (!defined $prop || $prop->{'type'} eq LITERAL) {
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
    my $pkg  = $prop->{'package'};
    my $class = $site->class($pkg);
    my $get_method = $self->can($prop->{'method'} || ($ttl ? 'cached' : 'object'));
    if ($rkey =~ /Ids$/) {
        no strict 'refs';
        *$AUTOLOAD = sub {
            my ($self) = @_;
            my @vals = map { $get_method->($self, $kind, $self->{$rkey}) } @$rval;
            $val = $self->{$key} = \@vals;
            return wantarray ? @vals : $val;
        };
    }
    else {
        no strict 'refs';
        *$AUTOLOAD = sub {
            my ($self) = @_;
            return $self->{$key} = $get_method->($self, $kind, $self->{$rkey});
        }
    }
    return if !defined $rval;  # NULL reference
    goto &$AUTOLOAD;
}

sub old_init {
    my ($self) = @_;
    return $self;
    # XXX
    my $site = $self->site;
    my @auto_deref = eval { $self->_auto_deref };
    foreach my $method (@auto_deref) {
        $self->$method;
        next;
    }
    return $self;
}

1;


