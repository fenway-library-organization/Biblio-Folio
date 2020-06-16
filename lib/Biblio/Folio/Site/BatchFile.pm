package Biblio::Folio::Site::BatchFile;

use strict;
use warnings;

use Biblio::Folio::Object;
use Biblio::Folio::Util qw(_run_hooks _int_set_str_to_hash);

use POSIX qw(strftime);

use vars qw(@ISA);
@ISA = qw(Biblio::Folio::Object);

sub file { @_ > 1 ? $_[0]{'file'} = $_[1] : $_[0]{'file'} }
sub fh { @_ > 1 ? $_[0]{'fh'} = $_[1] : $_[0]{'fh'} }
sub record_number { @_ > 1 ? $_[0]{'record_number'} = $_[1] : $_[0]{'record_number'} }
sub batch_number { @_ > 1 ? $_[0]{'batch_number'} = $_[1] : $_[0]{'batch_number'} }
sub profile { @_ > 1 ? $_[0]{'profile'} = $_[1] : $_[0]{'profile'} }

sub is_valid { @_ > 1 ? $_[0]{'is_valid'} = $_[1] : $_[0]{'is_valid'} }
sub results { @_ > 1 ? $_[0]{'results'} = $_[1] : $_[0]{'results'} }
sub errors { @_ > 1 ? $_[0]{'errors'} = $_[1] : $_[0]{'errors'} }

sub init {
    my ($self) = @_;
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

sub iterate {
    my $self = shift;
    local $_;
    unshift @_, 'file' if @_ % 2;
    my %arg = (%$self, @_);
    my $hooks = $arg{'hooks'};
    my $only = $arg{'only'};  # The only batch(es) to iterate over
    my ($site, $kind, $file, $fh, $batch_size, $limit) = @arg{qw(site kind file fh batch_size limit)};
    my ($begin, $before, $each, $error, $after, $end) = @$hooks{qw(begin before each error after end)};
    # die "no callback" if !defined $each;
    $batch_size ||= 1;
    my $max_errors = $limit ? $limit->{'errors'} : 1<<20;
    my $max_consecutive_errors = $limit ? $limit->{'consecutive_errors'} : 100;
    if (!defined $fh) {
        die "no file to iterate over" if !defined $file;
        $fh = $self->{'fh'} = $self->_open($file);
    }
    $only = _int_set_str_to_hash($only) if defined $only;
    my $batch_num = 0;   # Batch number (1..)
    my $n = 0;  # Record number within the batch (1..)
    my $num_consecutive_errors = 0;
    my $num_errors = 0;
    my @batch;
    my $success = 0;
    my %params = ('source' => $self, 'batch' => \@batch, map { $_ => $arg{$_} } qw(file format limit offset profile site kind));
    my $proc = sub {
        _run_hooks('begin'  => $begin,  %params, 'batch_num' => $batch_num, 'n' => $n, 'num_errors' => $num_errors, @_) if $n == 1;
        _run_hooks('before' => $before, %params, 'batch_num' => $batch_num, 'n' => $n, 'num_errors' => $num_errors, @_) if $before;
        _run_hooks('each'   => $each,   %params, 'batch_num' => $batch_num, 'n' => $n, 'num_errors' => $num_errors, @_) if $each;
        _run_hooks('after'  => $after,  %params, 'batch_num' => $batch_num, 'n' => $n, 'num_errors' => $num_errors, @_) if $after;
        @batch = ();
    };
    eval {
        while (1) {
            my ($obj, $ok);
            eval {
                $obj = $self->next($fh);
                ($ok, $num_consecutive_errors) = (1, 0);
            };
            if (defined $obj) {
                $n++;
                push @batch, defined $kind ? $site->object($kind, $obj) : $obj;
                if (@batch == $batch_size) {
                    $batch_num++;
                    if (!$only || $only->{$batch_num}) {
                        $proc->();
                    }
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
                    _run_hooks('error' => $error, %params, 'n' => $n, 'num_errors' => $num_errors);
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
        if (@batch) {
            $batch_num++;
            if (!$only || $only->{$batch_num}) {
                $proc->();
            }
        }
        _run_hooks('end' => $end, %params, 'n' => $n, 'num_errors' => $num_errors) if $n > 0;
    };
    $self->done(%arg);
    $self->results({
        'errors' => $num_errors,
    });
    return $self;
}

sub done {
    my ($self, %arg) = @_;
    close delete $self->{'fh'};
    delete $self->{'file'} if !defined $arg{'file'};
}

1;
