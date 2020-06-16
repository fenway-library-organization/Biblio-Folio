package Biblio::Folio;

use strict;
use warnings;

use Biblio::Folio::Util qw(_read_config);
use Biblio::Folio::Site;
use Biblio::Folio::Classes;

my $default_root ='/usr/local/folio';

sub new {
    my $cls = shift;
    my $self = bless {
        'root' => $ENV{'FOLIO_ROOT'} ||= $default_root,
        @_,
    }, $cls;
    $self->init;
    return $self;
}

sub root { @_ > 1 ? $_[0]{'root'} = $_[1] : $_[0]{'root'} }
sub json { @_ > 1 ? $_[0]{'json'} = $_[1] : $_[0]{'json'} }

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
    $self->{'json'} ||= JSON->new->pretty->canonical->convert_blessed;
    # $self->init_classes_and_properties;
}

sub site_names {
    my ($self) = @_;
    my $list = $self->file('site.list');
    open my $fh, '<', $list or die "open $list: $!";
    my @names = grep { /^\s*[^\n#]/ } <$fh>;
    chomp @names;
    return @names;
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
        'json' => $self->json,
    );
}

1;
