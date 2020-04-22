package Biblio::Folio::Site::BatchFile;

use strict;
use warnings;

use POSIX qw(strftime);
use Data::UUID;
use Biblio::Folio::Util qw(_run_hooks);

@Biblio::Folio::Site::BatchFile::ISA = qw(Biblio::Folio::Object);

sub file { @_ > 1 ? $_[0]{'file'} = $_[1] : $_[0]{'file'} }
sub fh { @_ > 1 ? $_[0]{'fh'} = $_[1] : $_[0]{'fh'} }
sub record_number { @_ > 1 ? $_[0]{'record_number'} = $_[1] : $_[0]{'record_number'} }
sub batch_number { @_ > 1 ? $_[0]{'batch_number'} = $_[1] : $_[0]{'batch_number'} }
sub profile { @_ > 1 ? $_[0]{'profile'} = $_[1] : $_[0]{'profile'} }
sub is_valid { @_ > 1 ? $_[0]{'is_valid'} = $_[1] : $_[0]{'is_valid'} }
sub errors { @_ > 1 ? $_[0]{'errors'} = $_[1] : $_[0]{'errors'} }

sub init {
    my ($self) = @_;
    my $ug = Data::UUID->new;
    $self->{'uuidgen'} = sub {
        return $ug->create_str;
    };
    return $self;
}

sub _open {
    my ($self, $file) = @_;
    die "no file to open"
        if !defined($file //= $self->file);
    open my $fh, '<', $file
        or die "open $file: $!";
    binmode $fh
        or die "binmode $file: $!";
    $self->file($file);
    $self->fh($fh);
    return $fh;
}

sub _uuid {
    my ($self, $obj) = @_;
    return $obj->{'id'}
        if defined $obj
        && defined $obj->{'id'};
    my $uuid = $self->{'uuidgen'}->();
    $obj->{'id'} = $uuid if defined $obj;
    return $uuid;
}

sub iterate {
    my $self = shift;
    local $_;
    unshift @_, 'file' if @_ % 2;
    my %arg = (%$self, @_);
    my ($site, $kind, $file, $fh, $batch_size, $limit, $begin, $before, $each, $error, $after, $end) = @arg{qw(site kind file fh batch_size limit begin before each error after end)};
    die "no callback" if !defined $each;
    $batch_size ||= 1;
    my $max_errors = $limit ? $limit->{'errors'} : 1<<31;
    my $max_consecutive_errors = $limit ? $limit->{'consecutive_errors'} : 100;
    if (!defined $fh) {
        die "no file to iterate over" if !defined $file;
        $fh = $self->{'fh'} = $self->_open($file);
    }
    my @batch;
    my $success = 0;
    my $n = 0;
    my %params = ('source' => $self, 'batch' => \@batch, map { $_ => $arg{$_} } qw(file format limit offset profile site kind));
    my $proc = sub {
        _run_hooks('begin'  => $begin,  %params, 'n' => $n, @_) if $n == 1;
        _run_hooks('before' => $before, %params, 'n' => $n, @_);
        _run_hooks('each'   => $each,   %params, 'n' => $n, @_);
        _run_hooks('after'  => $after,  %params, 'n' => $n, @_);
        @batch = ();
    };
    my $obj;
    eval {
        my $num_consecutive_errors = 0;
        my $num_errors = 0;
        while (1) {
            my $ok;
            eval {
                $obj = $self->_next($fh);
                ($ok, $num_consecutive_errors) = (1, 0);
            };
            if (defined $obj) {
                $n++;
                push @batch, defined $kind ? $site->object($kind, $obj) : $obj;
                $proc->() if @batch == $batch_size;
            }
            elsif ($ok) {
                # EOF
                last;
            }
            else {
                # Error reading the next record
                $n++;  # XXX Is this right?
                $num_errors++;
                $num_consecutive_errors++;
                my $die = 1;
                eval {
                    _run_hooks('error' => $error, %params, 'n' => $n);
                    $die = 0;
                };
                if ($die) {
                    my $msg = "error handler failed at $n";
                    my ($err) = split /\n/, $@;
                    $msg .= ': ' . $err if $err =~ /\S/;
                    die $msg;
                }
                die "maximum number of errors reached at $n"
                    if $num_errors >= $max_errors;
                die "maximum number of consecutive errors reached at $n"
                    if $num_consecutive_errors >= $max_consecutive_errors;
            }
        }
        $proc->() if @batch;
        _run_hooks('end' => $end, %params, 'n' => $n) if $n > 0;
        $success = 1;
    };
    delete $self->{'fh'};
    delete $self->{'file'} if !defined $arg{'file'};
    close $fh or $success = 0;
    die $@ if !$success;
    return $self;
}

### sub _default_user {
###     my ($self) = @_;
###     return {
###         'id' => undef,
###         'username' => undef,
###         'externalSystemId' => undef,
###         'barcode' => undef,
###         'active' => 1,
###         'type' => undef,
###         'patronGroup' => undef,
###         'enrollmentDate' => undef,
###         'expirationDate' => undef,
###         'personal' => {
###             'lastName' => undef,
###             'firstName' => undef,
###             'middleName' => undef,
###             'email' => undef,
###             'phone' => undef,
###             'mobilePhone' => undef,
###             'dateOfBirth' => undef,
###             'addresses' => undef,
###         },
###     };
### }

sub _req {
    my ($k, $v) = @_;
    die if !defined $v;
    return ($k => $v);
}

sub _opt {
    my ($k, $v) = @_;
    return if !defined $v;
    return ($k => $v);
}

1;
