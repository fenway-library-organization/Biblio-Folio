package Biblio::Folio::Object;

use strict;
use warnings;

use constant qw(LITERAL LITERAL);
use constant qw(UUID    UUID   );

use Biblio::Folio::Util qw(_camel);

# @Biblio::Folio::Object::Instance::ISA =
# @Biblio::Folio::Object::HoldingsRecord::ISA =
# @Biblio::Folio::Object::Item::ISA =
# @Biblio::Folio::Object::SourceRecord::ISA =
# @Biblio::Folio::Object::Location::ISA =
# @Biblio::Folio::Object::CallNumberType::ISA =
#     qw(Biblio::Folio::Object);

#*_camel = *Biblio::Folio::Site::_camel;

our $AUTOLOAD;

# sub ttl { 3600 }

sub new {
    my $cls = shift;
    my $self = bless { @_ }, $cls;
    $self->init;
    return $self;
}

sub DESTROY { }

sub init { }

sub _uri_create {
    my ($self) = @_;
    my $uri = $self->_uri;
    $uri =~ s{/%s$}{};
    return $uri;
}

sub _uri_update {
    my ($self) = @_;
    my $uri = $self->_uri;
    my $id = $self->id;
    $uri =~ s{/%s$}{$id}
        or die "no placeholder in uri $uri to use for updates";
    return $uri;
}

sub _uri_delete {
    my ($self) = @_;
    my $uri = $self->_uri;
    my $id = $self->id;
    $uri =~ s{/%s$}{$id}
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

### sub old_init {
###     my ($self) = @_;
###     my $site = $self->site;
###     my $cls = ref $self;
###     my $class = $site->class($cls);
###     my $wild  = $site->class('*');
###     my @blessings = (
###         @{ $wild->{'blessings'} ||= [] },
###         @{ $class->{'blessings'} ||= [] },
###     );
###     my %blessed;
###     foreach my $blessing (@blessings) {
###         my ($prop, $pkg, $each) = @$blessing{qw(property package each)};
###         next if $blessed{$prop}++;
###         my $propval = $self->{$prop};
###         next if !defined $propval;
###         my $propref = ref $propval;
###         my $bclass = $site->class($pkg);
###         if ($propref eq 'ARRAY' && $each) {
###             @$propval = map {
###                 die "can't bless a non-hash member of an array"
###                     if ref($_) ne 'HASH';
###                 bless $_, $pkg
###             } @$propval;
###         }
###         else {
###             if ($propref eq '') {
###                 print STDERR "DEBUG: $cls.$prop is scalar ($propval)\n";
###             }
###             else {
###                 print STDERR "DEBUG: $cls.$prop isa $propref\n"
###                     if $each || $propref ne 'HASH';
###                 $self->{$prop} = bless $propval, $pkg;
###             }
###         }
###     }
###     return $self;
### }

sub TO_JSON {
    my %self = %{ shift() };
    delete @self{grep { /^_/ || ref($self{$_}) !~ /^(?:ARRAY|HASH)?$/ } keys %self};
    return \%self;
}

### sub as_hash {
###     my %self = %{ shift() };
###     delete @self{grep { /^_/ } keys %self};
###     return \%self;
### }

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
###     if (!eval "keys %${pkg}::") {
###         $site->define_classes($pkg);
###     }
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

1;


