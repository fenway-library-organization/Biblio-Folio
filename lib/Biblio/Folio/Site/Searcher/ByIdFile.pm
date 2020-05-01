package Biblio::Folio::Site::Searcher::ByIdFile;

use strict;
use warnings;

use Biblio::Folio::Site::Searcher;

use vars qw(@ISA);

@ISA = qw(Biblio::Folio::Site::Searcher);

sub new {
    my $cls = shift;
    unshift @_, 'file' if @_ % 2;
    return $cls->SUPER::new(@_);
}

sub reader { @_ > 1 ? $_[0]{'reader'} = $_[1] : $_[0]{'reader'} }
sub searcher { @_ > 1 ? $_[0]{'searcher'} = $_[1] : $_[0]{'searcher'} }
sub id_buffer { @_ > 1 ? $_[0]{'id_buffer'} = $_[1] : $_[0]{'id_buffer'} }

sub uri {
    my $self = shift;
    my $uri = $self->SUPER::uri(@_);
    my $searcher = $self->searcher or return $uri;
    $searcher->uri(@_);
}

sub offset {
    my $self = shift;
    my $offset = $self->SUPER::offset(@_);
    my $searcher = $self->searcher or return $offset;
    $searcher->offset(@_);
}

sub limit {
    my $self = shift;
    my $limit = $self->SUPER::limit(@_);
    my $searcher = $self->searcher or return $limit;
    $searcher->limit(@_);
}

sub init {
    my ($self) = @_;
    $self->SUPER::init;
    my $file = $self->{'file'};
    die 'internal error: a batch ID searcher requires a file to read IDs from'
        if !defined $file;
    open my $fh, '<', $file or die "open $file: $!";
    $self->{'reader'} = sub {
        my $id = <$fh>;
        return if !defined $id;
        chomp $id;
        return $id;
    };
}

sub next {
    my ($self, $n) = @_;
    $n ||= 1;
    my @objects;
    my $searcher = $self->searcher;
    while (@objects < $n) {
        $searcher = $self->next_searcher if !$searcher || $searcher->finished;
        if (wantarray) {
            return @objects if !$searcher;
            my @more_objects = $searcher->next($n - @objects);
            undef($searcher), next if !@more_objects;
            push @objects, @more_objects;
        }
        elsif (!$searcher) {
            return if !@objects;
            return shift @objects;
        }
        else {
            my $obj = $searcher->next;
            undef $searcher;
            return $obj if $obj;
        }
    }
    return @objects;
}

sub read_more {
    my ($self) = @_;
    my $reader = $self->reader;
    my $limit = $self->limit;
    my @ids;
    while ($limit--) {
        my $id = $reader->();
        last if !defined $id;
        push @ids, $id;
    }
    return @ids;
}

sub next_searcher {
    my ($self) = @_;
    my @ids = $self->read_more;
    if (!@ids) {
        $self->finished(1);
        return;
    }
    my $site = $self->site;
    my $searcher = $site->searcher($self->kind, 'id' => [@ids]);
    $self->searcher($searcher);
    return $searcher;
}

1;
