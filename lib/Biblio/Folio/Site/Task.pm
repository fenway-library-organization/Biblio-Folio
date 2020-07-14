package Biblio::Folio::Site::Task;

use strict;
use warnings;

use Biblio::Folio::Util qw(_mkdirs _unique _uuid _json_write _json_read _use_class);
use DBI;

# States
use constant qw(SETUP setup);
use constant qw(READY ready);
use constant qw(BUSY busy);
use constant qw(IDLE idle);
use constant qw(DONE done);

# Succeeded?
use constant qw(OK     1);
use constant qw(FAILED 0);
use constant qw(STOPPED -1);

sub new {
    my $cls = shift;
    my %arg = @_;
    my $self = bless \%arg, $cls;
    $self->init;
    return $self;
}

sub dry_run { @_ > 1 ? $_[0]{'dry_run'} = $_[1] : $_[0]{'dry_run'} }
sub verbose { @_ > 1 ? $_[0]{'verbose'} = $_[1] : $_[0]{'verbose'} }

sub init {
    my ($self) = @_;
    return $self;
}

sub step_handler {
    my $self = shift;
    my $step = shift;
    return $self->{'step_handler'}{$step};
}

sub path {
    my ($self, $path) = @_;
    return $path if $path =~ m{^/};
    return $self->{'root'} . '/' . $path;
}

sub directory {
    my ($self, $path) = @_;
    $path = $self->{'root'} . '/' . $path if $path !~ m{^/};
    _mkdirs($path);
    return $path;
}

sub state {
    my $self = shift;
    return $self->{'state'} if !@_;
    my $new = shift;
    my $old = $self->{'state'};
    my $data = [@_];
    my ($old_dir, $new_dir) = map { $self->path('@'.$_) } $old, $new;
    rename $old_dir, $new_dir
        or die "rename $old_dir $new_dir: $!";
    _json_write("$new_dir/$new.json", $data); 
    return $self->{'state'} = $new;
}

sub steps {
    my $self = shift;
    return @{ $self->{'steps'} = [@_] } if @_;
    return @{ $self->{'steps'} } if $self->{'steps'};
    return ref($self)->all_steps;
}

sub all_steps { () }

sub work {
    my $self = shift;
    return $self->{'work'} ||= [@_];
}

sub files {
    my $self = shift;
    my $site = $self->site;
    return map {
        {
            'site' => $site,
            'task' => $self,
            'path' => $_,
        }
    } @_;
}

sub kind {
    die "abstract class " . __PACKAGE__ . " has no kind";
}

sub worker {
    my ($self, $actor) = @_;
    my $ctx = $self->context;
    my $kind = $ctx->{'kind'} ||= $self->kind;
    my $worker = $self->{'workers'}{$actor};
    return $worker if $worker;
    my $profile = $self->profile;
    my $site = $self->site;
    my $pkg = $site->class_for($kind, $actor, $profile);
    _use_class($pkg);
    return $self->{'workers'}{$actor} = $pkg->new(
        %$ctx,
        'site' => $self->site,
        'profile' => $self->profile,
        'kind' => $kind,
    );
}

sub old_worker {
    my ($self, $actor, $kind) = @_;
    my $site = $self->site;
    my $method = $actor . '_for';
    my %arg = (%$self, @_);
    return $self->{'workers'}{$actor} = $site->$method($self->kind, %arg);
}

sub record {
    my ($self, $step, $status, @work) = @_;
    $self->{'status'}{$step} = [@work];
    return $self;
}

sub run {
    my ($self, %arg) = @_;
    my $work = $self->setup(%arg);
    $self->start($work);
    my $status;
    foreach my $step ($self->steps) {
        eval {
            my $code = $self->step_handler($step);
            ($status, @$work) = $code->($self, @$work);
        };
        $status = FAILED if !defined $status;
        $self->record($step, $status, @$work);
        return $self->fail(@$work)
            if $status == FAILED;
        return $self->stop(@$work)
            if $status == STOPPED;
    }
    return $self->succeed(@$work);
}

sub setup {
    my ($self, %arg) = @_;
    $self->{'t0'} = time;
    my $work = $arg{'work'} ||= $self->{'work'} ||= [%arg];
    my $steps = $arg{'steps'};
    if (defined $steps) {
        my $r = ref $steps;
        if ($r eq '' && $steps =~ /^(?:-|\.\.?)(\S+)$/) {
            my $last_step = $1;
            undef $steps;
            my @steps;
            foreach my $step ($self->steps) {
                push @steps, $step;
                $steps = \@steps, last if $step eq $last_step;
            }
            die "no such step: $last_step" if !$steps;
        }
        elsif ($r ne 'ARRAY') {
            die "invalid steps: $steps";
        }
    }
    my $ok;
    eval {
        my @steps = _unique(
            'begin',
            $steps ? @$steps : $self->steps,
            'end'
        );
        my %step_handler = map {
            $_ => $self->can('step_'.$_) || die "no such task step: $_"
        } $self->all_steps;
        $self->{'step_handler'} = \%step_handler;
        my $last_step = $steps[-1];
        $self->state(READY);
        $ok = 1;
    };
    return $work if $ok;
    $arg{'error'} = $@;
    $self->fail(@$work);
}

sub start {
    my ($self, $work) = @_;
    $self->work($work);
    $self->state(BUSY);
    $self->{'t0'} = time;
    return $self;
}

sub stop {
    my ($self, @work) = @_;
    $self->work([@work]);
    $self->state(IDLE);
    my $tn = $self->{'tn'} = time;
    $self->{'elapsed'} += $tn - $self->{'t0'};
    return $self;
}

sub finish {
    my ($self, $succeeded, @results) = @_;
    my $tn = $self->{'tn'} = time;
    $self->{'elapsed'} += $tn - $self->{'t0'};
    $self->{'succeeded'} = $succeeded;
    $self->{'results'} = [@results];
    $self->state(DONE, @results);
    return $self;
}

sub succeed {
    my ($self, @results) = @_;
    return $self->finish(OK, @results);
}

sub fail {
    my ($self, @results) = @_;
    return $self->finish(FAILED, @results);
}

# --- Default steps

sub step_begin {
    my ($self, $work) = @_;
    return $work;
}

sub step_end {
    my ($self, $work) = @_;
    return $work;
}

1;
