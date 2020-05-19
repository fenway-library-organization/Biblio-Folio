package Biblio::Folio::Site::MARC;

use strict;
use warnings;

use MARC::Loop qw(marcloop marcparse marcfield marcbuild TAG DELETE VALREF IND1 IND2 SUBS);
use Scalar::Util qw(blessed);

sub new {
    my $cls = shift;
    if (@_ % 2) {
        my $r = ref $_[0];
        if ($r eq 'SCALAR') {
            unshift @_, 'marcref';
        }
        elsif ($r eq '') {
            my $marc = shift;
            unshift @_, 'marcref' => \$marc;
        }
        else {
            die 'invalid parameters given to ' .  __PACKAGE__ . '::new';
        }
    }
    my $self = bless { @_ }, $cls;
    $self->init;
    return $self;
}

sub leader { @_ > 1 ? $_[0]{'leader'} = $_[1] : $_[0]->parse->{'leader'} || _default_leader() }
sub fields { @_ > 1 ? $_[0]{'fields'} = $_[1] : $_[0]->parse->{'fields'} || [] }
sub marcref { @_ > 1 ? $_[0]{'marcref'} = $_[1] : $_[0]{'marcref'} }
sub status { @_ > 1 ? substr($_[0]{'leader'},5,1) = $_[1] : substr($_[0]->parse->{'leader'},5,1) }

sub instance { @_ > 1 ? $_[0]{'instance'} = $_[1] : $_[0]{'instance'} }
sub source_record { @_ > 1 ? $_[0]{'source_record'} = $_[1] : $_[0]{'source_record'} }

sub is_parsed { @_ > 1 ? $_[0]{'is_parsed'} = $_[1] : $_[0]{'is_parsed'} }
sub is_dirty { @_ > 1 ? $_[0]{'is_dirty'} = $_[1] : $_[0]{'is_dirty'} }

sub init {
    my ($self) = @_;
    $self->{'is_parsed'} = 0;
    $self->{'is_dirty'} = 0;
}

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
    $self->{'fields'} = [ map {
        Biblio::Folio::Site::MARC::Field->new($self, $_)
    } @$fields ];
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

sub add_metadata {
    my ($self, %arg) = @_;
    my ($s, $i, $x, $d) = @arg{qw(source_record_id instance_id suppressed deleted)};
    die "no instance or source record IDs to insert into MARC record"
        if !defined $s || !defined $i;
    my ($leader, $fields) = ($self->leader, $self->fields);
    my @subs = ( 'i' => $i, 's' => $s );
    # TODO Don't force marcbuild if not necessary
    push @subs, 'x' => 'suppressed' if $x;
    push @subs, 'd' => 'deleted'    if $d;
    my ($old999) = grep { $_->[TAG] eq '999' && $_->[IND1] eq 'f' && $_->[IND2] eq 'f' } @$fields;
    if ($old999) {
        my @oldsubs = @$old999[SUBS..$#$old999];
        if (@oldsubs == @subs) {
            foreach (0..$#oldsubs) {
                my ($old, $new) = ($oldsubs[$_], $subs[$_]);
                if ($old ne $new) {
                    undef $old999;
                    last;
                }
            }
        }
        else {
            undef $old999;
        }
    }
    if (@subs && !$old999) {
        my $marcref = $self->marcref;
        my ($leader, $fields) = marcparse($marcref);
        $self->leader($leader);
        $self->fields($fields);
        @$fields = (
            (grep { $_->[TAG] lt '999' && !$_->[DELETE] } @$fields),
            (grep { $_->[TAG] eq '999' && !$_->[DELETE] && $_->[IND1] ne 'f' && $_->[IND2] ne 'f' } @$fields),
            marcfield('999', 'f', 'f', @subs),
            (grep { $_->[TAG] gt '999' && !$_->[DELETE] } @$fields),
        );
        $self->dirty(1);
    }
    return $self;
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

package Biblio::Folio::Site::MARC::Field;

use MARC::Loop qw(marcloop marcparse marcfield marcbuild TAG DELETE VALREF IND1 IND2 SUBS SUB_ID SUB_VALREF);

sub new {
# Control fields:
#     $field = Biblio::Folio::Site::MARC::Field->new($tag, $value);  
#     $field = Biblio::Folio::Site::MARC::Field->new(marcfield(...));
#     $field = Biblio::Folio::Site::MARC::Field->new($record, $tag, $value);  
#     $field = Biblio::Folio::Site::MARC::Field->new($record, marcfield(...));
# Data fields:
#     $field = Biblio::Folio::Site::MARC::Field->new($tag, $ind1, $ind2, @subfields);  
#     $field = Biblio::Folio::Site::MARC::Field->new(marcfield(...));
#     $field = Biblio::Folio::Site::MARC::Field->new($record, $tag, $ind1, $ind2, @subfields);  
#     $field = Biblio::Folio::Site::MARC::Field->new($record, marcfield(...));
    my $cls = shift;
    my %self;
    $self{'record'} = shift if @_ && blessed($_[0]) && $_[0]->isa('Biblio::Folio::Site::MARC');
    if (@_ == 1) {
        my $ary = shift;
        die "not the output of marcfield(...)" if ref($ary) ne 'ARRAY';
        $self{'content'} = [ @$ary ];  # Shallow copy
    }
    elsif (@_ > 1) {
        my @content;
        my $tag = $content[TAG] = shift;
        if ($tag lt '010') {
            my $val = shift;
            die "control field $tag with data field elements?"
                if @_;
            $content[VALREF] = ref($val) ? $val : \$val;
        }
        else {
            my ($ind1, $ind2, @subs) = @_;
            my $num_subs = @subs >> 1;
            warn "data field $tag with no subfields?"
                if !@subs;
            my $val = $ind1 . $ind2;
            $content[VALREF] = \$val;
            my $subnum = 0;
            while (@subs > 1) {
                my ($subid, $subval) = splice @subs, 0, 2;
                $content[SUBS+$subnum] = [$subid, \$subval];
                $val .= "\x1f" . $subid . $subval;
            }
            die "data field with half-subfield?"
                if @subs;
            @content[VALREF, IND1, IND2] = (\$val, $ind1, $ind2);
        }
        $self{'content'} = \@content;
    }
    else {
        die "field without a tag?";
    }
    return bless \%self, $cls;
}

sub tag { shift->{'tag'} }
sub set_tag {
    my ($self, $new_tag) = @_;
    my $old_tag = $self->{'tag'};
    if (($old_tag < '010') xor ($new_tag < '010')) {
        die "can't change a control field to a data field, or vice-versa";
    }
    $self->{'content'}[TAG] = $new_tag;
    $self->{'is_dirty'} = 1;
}

sub indicator {
    my ($self, $i) = @_;
    my $content = $self->{'content'};
    return undef if $content->[TAG] lt '010';
    return $i == 1 ? $content->[IND1]
         : $i == 2 ? $content->[IND2]
         : die "invalid indicator number: $i";
}
sub set_indicator {
    my ($self, $i, $v) = @_;
    die "invalid indicator number: $i"
        if $i !~ /^[12]$/;
    $self->{'content'}[IND1+$i-1] = $v;
    $self->{'is_dirty'} = 1;
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

sub subfield {
    my ($self, $id) = @_;
    die if !defined $id;
    my $content = $self->{'content'};
    my @subs = map { $_->[SUB_ID] eq $id ? (${ $_->[SUB_VALREF] }) : () } @$content[SUBS..$#$content];
    return @subs if wantarray;
    return @subs ? shift @subs : undef;
}

1;
