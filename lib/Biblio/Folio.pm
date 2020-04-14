package Biblio::Folio;

use strict;
use warnings;

use Data::UUID;
use Biblio::Folio::Util qw(_read_config);
use Biblio::Folio::Site;
use Biblio::Folio::Classes;

my $ug = Data::UUID->new;

sub new {
    my $cls = shift;
    my $self = bless {
        'root' => '/usr/local/folio',
        @_,
    }, $cls;
    $self->init;
    return $self;
}

sub root { @_ > 1 ? $_[0]{'root'} = $_[1] : $_[0]{'root'} }

sub file {
    my ($self, $path) = @_;
    $path = $self->{'root'} . '/' . $path if $path !~ m{^/};
    my @files = glob($path);
    return @files if wantarray;
    die "multiple files for $path" if @files > 1;
    return if !@files;
    return $files[0];
}

sub config {
    my ($self, $key) = @_;
    my $config = $self->{'config'};
    return $config if @_ == 1;
    return $config->{$key};
}

sub site {
    my ($self, $name, @args) = @_;
    return Biblio::Folio::Site->new(
        @args,
        'name' => $name,
        'folio' => $self,
    );
}

sub init {
    my ($self) = @_;
    my $root = $self->root;
    my $config = $self->{'config'} ||= {};
    my @files = $self->file('conf/*.conf');
    foreach my $file (@files) {
        $file =~ m{/([^/.]+)\.conf$};
        my $name = $1;
        undef $name if $name eq 'folio';
        _read_config($file, $config, $name);
    }
    # $self->init_classes_and_properties;
}

# sub init_classes_and_properties {
#     my ($self) = @_;
#     my $classes = $self->config('classes');
#     my (%pkg2class, %prop2pkg, %prop2class, %blessing);
#     while (my ($k, $class) = each %$classes) {
#         my $kind = Biblio::Folio::Util::_pkg2kind($k);
#         my $pkg = Biblio::Folio::Util::_kind2pkg($kind);
#         my $ttl = $class->{'ttl'} ||= 1;
#         my @refs = split(/,\s*/, $c->{'references'} || '');
#         my %uri = %{ $class->{'uri'} ||= {} }; 
#         foreach my $action (sort keys %uri) {
#             my $uri = delete $uri{$action}
#                 or next;
#             $uri =~ s/{[^{}]+}/%s/;
#             $uri{$action} = $uri;
#         }
#         $class->{'references'} = \@refs;
#         $class->{'uri'} = \%uri;
#         foreach my $ref (@refs) {
#             die "reference property $ref redefined" if exists $prop2class{$ref};
#             $prop2pkg{$ref} = $pkg;
#         }
#         my @blessed_refs;
#         foreach (split(/,\s*/, delete($c->{'blessedReferences'}) || '')) {
#             /^(\*|[a-z][A-Za-z]*)\.([a-z][A-Za-z]*)(\[\])?$/
#                 or die "bad blessed reference in class $pkg: $_";
#             my ($from_kind, $from_property, $each) = ($1, $2, defined $3);
#             my $from_pkg = $from_kind eq '*' ? '*' : Biblio::Folio::Util::_kind2pkg($from_kind);
#             my $blessing = {
#                 'kind' => $kind,
#                 'package' => $pkg,
#                 'property' => $from_property,
#                 'each' => $each,
#             };
#             push @{ $blessing{$from_pkg} ||= [] }, $blessing;
#         }
#     }
#     while (my ($k, $c) = each %$classes) {
#         next if $k eq '*';
#         my $pkg = $c->{'package'};
#         my @blessings = @{ $blessing{$pkg} || [] };
#         my $class = $class{$pkg} = Biblio::Folio::Class->new(
#             'site' => $self,
#             'blessed_references' => \@blessings,
#             %$c,
#         );
#         my $ok;
#         eval { eval "use $pkg"; $ok = 1 };
#         if (!$ok) {
#             die $@;
#         }
#     }
#     while (my ($p, $pkg) = each %prop2pkg) {
#         $prop2class{$p} = $class{$pkg}
#             or die "no such class: $pkg";
#     }
#     $self->{'classes'} = \%class;
#     $self->{'properties'} = \%prop2class;
# }

sub uuid {
    return $ug->create_str;
}

1;
