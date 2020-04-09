package Biblio::Folio::MARC;

use strict;
use warnings;

use MARC::Loop qw(marcloop marcparse marcfield marcbuild TAG DELETE VALREF IND1 IND2 SUBS);

@Biblio::Folio::MARC::ISA = qw(Biblio::Folio::Object);

sub leader { @_ > 1 ? $_[0]{'leader'} = $_[1] : $_[0]->parse->{'leader'} || _default_leader() }
sub fields { @_ > 1 ? $_[0]{'fields'} = $_[1] : $_[0]->parse->{'fields'} || [] }
sub marcref { @_ > 1 ? $_[0]{'marcref'} = $_[1] : $_[0]{'marcref'} }
sub dirty { @_ > 1 ? $_[0]{'dirty'} = $_[1] : $_[0]{'dirty'} }
sub status { @_ > 1 ? substr($_[0]{'leader'},5,1) = $_[1] : substr($_[0]->parse->{'leader'},5,1) }

sub instance { @_ > 1 ? $_[0]{'instance'} = $_[1] : $_[0]{'instance'} }
sub source_record { @_ > 1 ? $_[0]{'source_record'} = $_[1] : $_[0]{'source_record'} }

sub parse {
    my ($self, $marc) = @_;
    my $marcref;
    if (defined $marc) {
        my $r = ref $marc;
        $marcref =
            $r eq ''       ? \$marc :
            $r eq 'SCALAR' ? $marc  :
            die "unparseable: $marc";
    }
    elsif ($self->{'is_parsed'}) {
        return $self;
    }
    else {
        $marcref = $self->{'marcref'}
            or die "nothing to parse: $self";
    }
    my ($leader, $fields) = marcparse($marcref);
    $self->{'leader'} = $leader;
    $self->{'fields'} = [ map { Biblio::Folio::MARC::Field->new($_) } @$fields ];
    $self->{'is_parsed'} = 1;
    return $self;
}

sub field {
    # @fields = $marc->field($tag_or_coderef_or_regexp);
    # $field = $marc->field($tag_or_coderef_or_regexp);
    my ($self, $what, $first) = @_;
    $self->parse if !$self->{'is_parsed'};
    my @fields = grep { !$_->[DELETE] } @{ $self->{'fields'} };
    my $ref = ref $what;
    if ($ref eq '') {
        @fields = grep { $_->[TAG] eq $what } @fields;
    }
    elsif ($ref eq 'CODE') {
        @fields = grep { $what->() } @fields;
    }
    elsif ($ref eq 'Regexp') {
        @fields = grep { $_ =~ $what } @fields;
    }
    else {
        die "usage: \$marc->field($what)";
    }
    return if !@fields;
    if (!$first) {
        return @fields if wantarray;
        die "multiple fields returned" if @fields > 1;
        return $fields[0];
    }
    elsif (wantarray) {
        return @fields if $first >= @fields;
        return @fields[0..($first-1)];
    }
    else {
        die "huh?" if $first > 1;
        die "multiple fields returned" if @fields > 1;
        return $fields[0];
    }
}

sub delete {
    my $self = shift;
    $_->[DELETE] = 1 for @_;
    return $self;
}

sub add_identifiers {
    my ($self, %arg) = @_;
    my $instance = $arg{'instance'} || $self->instance;
    my $source_record = $arg{'source_record'} || $self->source_record;
    my $i = $instance ? $instance->id : $arg{'instance_id'};
    my $s = $source_record ? $source_record->id : $arg{'source_record_id'};
    my %sub = (
        defined $i ? ('i' => $i) : (),
        defined $s ? ('s' => $s) : (),
    );
    if (!keys %sub && $arg{'strict'}) {
        die "no instance or source record IDs to insert into MARC record"
    }
    my $fields = $self->fields;
    my ($old999) = grep {
        $_->[TAG] eq '999' &&
        $_->[IND1] eq 'f'  &&
        $_->[IND2] eq 'f'
    } @$fields;
    my %set;
    my @new999 = qw(999 f f);
    foreach (sort keys %sub) {
        $set{$_} = 1;
        push @new999, $_ => $sub{$_};
    }
    my $dirty;
    my $leader;
    if (keys %set) {
        my $marcref = $self->marcref;
        ($leader, $fields) = marcparse($marcref);
        $dirty = 1;
        if ($old999) {
            my $valref = $old999->[VALREF];
            while ($$valref =~ /\x1f([is])([^\x1d-\x1f]+)/g) {
                my $val = $sub{$_} or next;
                delete $set{$1} if $2 eq $val;
            }
            $old999->[DELETE] = $dirty = keys %set;
        }
    }
    elsif ($arg{'strict'}) {
        die "no instance or source record IDs to insert into MARC record"
    }
    if ($dirty) {
        @$fields = (
            (grep { $_->[TAG] lt '999' && !$_->[DELETE] } @$fields),
            marcfield(@new999),
            (grep { $_->[TAG] gt '999' && !$_->[DELETE] } @$fields),
        );
        push @$fields, marcfield(@new999);
        $self->dirty(1);
    }
}

sub stub {
    my ($self, %arg) = @_;
    my ($leader, $status, $instance) = @arg{qw(leader status instance)};
    if (ref $self) {
        $leader ||= $self->leader;
        $instance ||= $self->instance;
    }
    else {
        $leader ||= _default_leader();
    }
    if (defined $status) {
        die "invalid status: $status" if $status !~ /^[a-z]$/;
        substr($leader, 5, 1) = $status;
    }
    my @fields = $instance ? (marcfield('001', $instance->id)) : ();
    return marcbuild($leader, \@fields);
}

sub as_marc21 {
    my ($self) = @_;
    my $marcref = $self->marcref;
    return marcbuild($self->leader, $self->fields) if $self->dirty || !$marcref;
    return $$marcref;
}

sub _default_leader {
    return '00000cam a2200000 a 4500';
}

package Biblio::Folio::MARC::Field;

use MARC::Loop qw(marcloop marcparse marcfield marcbuild TAG DELETE VALREF IND1 IND2 SUBS);

sub new {
    # $field = Biblio::Folio::MARC::Field->new($record->field(...));
    my ($cls, $ary) = @_;
    # Shallow clone of the array ref
    return bless [ @$ary ], $cls;
}

sub indicators {
    my $self = shift;
    return $self->[IND1] . $self->[IND2] if !@_;
    my $inds = shift;
    my $num_inds = length $inds;
    die "too many indicators" if $num_inds > 2;
    die "not enough indicators" if $num_inds < 2;
    @$self[IND1, IND2] = split //, $inds;
    return $inds;
}

1;
