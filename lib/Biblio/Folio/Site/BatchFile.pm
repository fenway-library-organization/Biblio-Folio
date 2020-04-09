package Biblio::Folio::Site::BatchFile;

use strict;
use warnings;

use POSIX qw(strftime);
use Data::UUID;

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

sub _run_hooks {
    my $hooks = shift
        or return;
    my $r = ref $hooks;
    if ($r eq 'CODE') {
        $hooks->(@_);
    }
    elsif ($r eq 'ARRAY') {
        $_->(@_) for @$hooks;
    }
    else {
        die "unrunnable hook: $r";
    }
}

sub iterate {
    my $self = shift;
    local $_;
    my %arg = (%$self, @_);
    unshift @_, 'file' if @_ % 2;
    my ($file, $fh, $batch_size, $limits, $first, $before, $each, $error, $after, $last) = @arg{qw(file fh batch_size limits first before each error after last)};
    die "no callback" if !defined $each;
    $batch_size ||= 1;
    my $max_errors = $limits ? $limits->{'errors'} : 1<<31;
    my $max_consecutive_errors = $limits ? $limits->{'consecutive_errors'} : 100;
    if (!defined $fh) {
        die "no file to iterate over" if !defined $file;
        $fh = $self->{'fh'} = $self->_open($file);
    }
    my @batch;
    my $success = 0;
    my $n = 0;
    eval {
        my $num_consecutive_errors = 0;
        my $num_errors = 0;
        while (1) {
            my ($user, $ok);
            eval {
                $user = $self->_next($fh);
                ($ok, $num_consecutive_errors) = (1, 0);
            };
            if (defined $user) {
                $n++;
                push @batch, $user;
                _run_hooks($first, $self) if $n == 1;
                if (@batch == $batch_size) {
                    _run_hooks($before);
                    _run_hooks($each, @batch);
                    _run_hooks($after);
                    @batch = ();
                }
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
                    _run_hooks($error, $n);
                    $die = 0;
                };
                if ($die) {
                    my $msg = "error handler failed at $n";
                    ($err) = split /\n/, $@;
                    $msg .= ': ' . $err if $err =~ /\S/;
                    die $msg;
                }
                die "maximum number of errors reached at $n"
                    if $num_errors >= $max_errors;
                die "maximum number of consecutive errors reached at $n"
                    if $num_consecutive_errors >= $max_consecutive_errors;
            }
        }
        if (@batch) {
            _run_hooks($before);
            _run_hooks($each, @batch);
            _run_hooks($after);
        }
        _run_hooks($last, $self) if $n > 0;
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
