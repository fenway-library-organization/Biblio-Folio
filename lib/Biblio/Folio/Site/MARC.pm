package Biblio::Folio::Site::MARC;

use strict;
use warnings;

use Biblio::Folio::Util qw(_optional);
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
    #if (!is_utf8($$marcref)) {
    #    $$marcref = decode('UTF-8', $$marcref);
    #}
    #my $lenstr = substr($$marcref, 0, 5);
    #if ($lenstr + 0 != length $$marcref) {
    #    substr($$marcref, 0, 5) = sprintf '%05d', length $$marcref;  # XXX
    #    $self->{'is_dirty'} = 1;
    #}
    my ($leader, $fields) = marcparse($marcref);
    $self->{'leader'} = $leader;
    $self->{'fields'} = [ map {
        _make_field($self, $_)
    } @$fields ];
    $self->{'is_parsed'} = 1;
    return $self;
}

sub field {
    # @fields = $marc->field($tag_or_coderef_or_regexp);
    # $field = $marc->field($tag_or_coderef_or_regexp);
    my ($self, $what, $first) = @_;
    $self->parse if !$self->{'is_parsed'};
    my @fields = grep { !$_->{'content'}[DELETE] } @{ $self->{'fields'} };
    my $ref = ref $what;
    if ($ref eq '') {
        @fields = grep { $_->{'content'}[TAG] eq $what } @fields;
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

sub add_metadata {
    my ($self, %arg) = @_;
    my ($s, $i, $x, $d) = @arg{qw(source_record_id instance_id suppressed deleted)};
    die "no instance or source record IDs to insert into MARC record"
        if !defined $s || !defined $i;
    my ($leader, $fields) = ($self->leader, $self->fields);
    my @subs = ( 'i' => $i, 's' => $s );
    # TODO Don't force marcbuild if not necessary
    push @subs, 'z' => 'suppressed' if $x;
    push @subs, 'z' => 'deleted'    if $d;
    my ($old999) = grep { $_->{'content'}[TAG] eq '999' && $_->{'content'}[IND1] eq 'f' && $_->{'content'}[IND2] eq 'f' } @$fields;
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
            (grep { $_->{'content'}[TAG] lt '999' && !$_->{'content'}[DELETE] } @$fields),
            (grep { $_->{'content'}[TAG] eq '999' && !$_->{'content'}[DELETE] && $_->{'content'}[IND1] ne 'f' && $_->{'content'}[IND2] ne 'f' } @$fields),
            marcfield('999', 'f', 'f', @subs),
            (grep { $_->{'content'}[TAG] gt '999' && !$_->{'content'}[DELETE] } @$fields),
        );
        $self->is_dirty(1);
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
    my @fields = $instance ? (marcfield('001', $instance->hrid)) : ();
    return marcbuild($leader, \@fields);
}

sub as_marc21 {
    my ($self) = @_;
    my $marcref = $self->marcref;
    my $dirty = $self->is_dirty;
    my @fields = map {
        $dirty = 1 if $_->{'is_dirty'};
        $_->{'content'}
    } @{ $self->fields };
    return $$marcref if $marcref && !$dirty;
    return marcbuild($self->leader, \@fields);
}

sub _default_leader {
    return '00000cam a2200000 a 4500';
}

sub new_field {
    my $self = shift;
    return _make_field($self, @_);
}

sub garnish {
    my ($self, %arg) = @_;
    my ($instance, $source_record, $mapping) = @arg{qw(instance source_record mapping)};
    die 'not yet implemented: $marc->garnish(mapping => {...}, ...)'
        if $mapping && keys %$mapping;
    my ($id, $hrid, $sid, $suppressed, $deleted);
    if (defined $instance) {
        ($id, $hrid, $suppressed, $deleted) = @$instance{qw(id hrid discoverySuppress deleted)};
        $sid = $source_record->id if $source_record;
    }
    else {
        ($id, $hrid, $sid, $suppressed, $deleted) = @arg{qw(instance_id instance_hrid source_record_id suppressed deleted)};
    }
    substr($self->{'leader'}, 5, 1) = 'd' if $deleted;
    $self->delete_fields(qw(001 003), sub { $_[0]->tag eq '999' && $_[0]->indicators eq 'ff' });
    $self->add_fields(
        _make_field($self, '001', $hrid),
        _make_field($self, '999', 'f', 'f',
            'i' => $id,
            _optional('s' => $sid),
            _optional('z' => $suppressed ? 'suppressed' : undef),
            _optional('z' => $deleted    ? 'deleted'    : undef),
        )
    );
    return $self;
}

sub _make_field {
    unshift @_, 'Biblio::Folio::Site::MARC::Field';
    goto &Biblio::Folio::Site::MARC::Field::new;
}

sub add_fields {
    my $self = shift;
    my $fields = $self->fields;
    my @fields;
    my @add = sort { $a->[0] cmp $b->[0] } map { [$_->{'content'}[TAG], $_] } @_;
    my @existing_fields = @$fields;
    while (@existing_fields && @add) {
        my $existing_tag = $existing_fields[0]{'content'}[TAG];
        my ($add_tag, $add_field) = @{ $add[0] };
        if ($add_tag lt $existing_tag) {
            shift @add;
            push @fields, $add_field;
        }
        else {
            push @fields, shift @existing_fields;
        }
    }
    push @fields, @existing_fields, map { $_->[1] } @add;
### foreach my $existing_field (@$fields) {
###     my $existing_content = $existing_field->{'content'};
###     my $existing_tag = $existing_content->[TAG];
###     if (@add && $add[0][0] le $existing_tag) {
###         my $add = shift @add;
###         my ($add_tag, $add_field) = @$add;
###         $add_field->is_deleted(0);
###         push @fields, $add_field;
###     }
###     else {
###         push @fields, $existing_field;
###     }
### }
### push @fields, map { $_->[1] } @add;
    @$fields = @fields;
    $self->is_dirty(1);
}

sub delete_fields {
    my $self = shift;
    my $n = 0;
    my $ok;
    eval {
        my $fields = $self->fields;
        my @conditions;
        foreach my $what (@_) {
            my $r = ref $what;
            if ($r eq '') {
                push @conditions, sub { shift()->tag eq $what };
            }
            elsif ($r eq 'Regexp') {
                push @conditions, sub { shift()->tag =~ $what };
            }
            elsif ($r eq 'CODE') {
                push @conditions, $what;
            }
            else {
                die "unknown field specifier type: $r";
            }
        }
        my @fields;
        foreach my $field (@$fields) {
            my $content = $field->{'content'};
            next if $content->[DELETE];
            foreach my $cond (@conditions) {
                next if !$cond->($field);
                $content->[DELETE] = 1;
                $n++;
                last;
            }
        }
        $ok = 1;
    };
    die "wtf?" if !$ok;
    return $n;
}

### sub add_hrid {
###     my $self = shift;
###     my ($tag, $sub) = @_
###     my $hrid = $instance->hrid;
###     my $fields = $self->fields;
###     push @$fields, _make_field($self, '901', ' ', ' ', 'h' => $hrid);
###     $self->is_dirty(1);
### }

sub add_holdings {
    my $self = shift;
    my %arg = @_;
    my ($holdings, $spell_out_locations, $add_items) = @arg{qw(holdings spell_out_locations add_items)};
    my $num_holdings = 0;
    my $num_items = 0;
    my $fields = $self->fields;
    my @add;
    foreach my $holding (@$holdings) {
        my $location = $holding->location;
        my $suppressed = $holding->discoverySuppress;
        my $locstr = $spell_out_locations
            ? $location->discoveryDisplayName // $location->name
            : $location->code;
        my $call_number = $holding->call_number;
        undef $call_number if defined $call_number && $call_number !~ /\S/;
        push @add, _make_field($self, 
            '852', ' ', ' ',
            'b' => $locstr,
            _optional('h' => $call_number),
            _optional('z' => $suppressed ? 'suppressed' : undef),
            '0' => $holding->id,
        );
        $num_holdings++;
        if ($add_items) {
            my @item_fields = _make_item_fields($self, $holding);
            push @add, @item_fields;
            $num_items += @item_fields;
        }
    }
    if (@add) {
        $self->delete_fields('852');
        $self->add_fields(@add) if @add;
        $self->is_dirty(1);
    }
    return ($num_holdings, $num_items) if wantarray;
    return $num_holdings;
}

sub delete_holdings {
    my ($self) = @_;
    $self->delete_fields('852');
    return $self;
}

sub _make_item_fields {
    my ($self, $holding) = @_;
    my @items = $holding->items;
    return if !@items;
    my $call_number = $holding->call_number;
    undef $call_number if defined $call_number && $call_number !~ /\S/;
    my @add;
    foreach my $item (@items) {
        my $iloc = $item->location->code;
        my $vol = $item->volume;
        # my $year = $item->year_caption;
        # my $copies = @{ $item->copy_numbers || [] };
        my $item_call_number = join(' ', grep { defined && length } $call_number, $vol);
        undef $item_call_number if $item_call_number !~ /\S/;
        push @add, _make_field($self,
            '859', ' ', ' ',
            'b' => $iloc,
            defined($item_call_number) ? ('h' => $item_call_number) : (),
            '0' => $item->id,
        );
    }
    return @add;
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
    $self{'record'} = shift if @_ && Scalar::Util::blessed($_[0]) && $_[0]->isa('Biblio::Folio::Site::MARC');
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
            my $num_subs = 0;
            warn "data field $tag with no subfields?"
                if !@subs;
            my $val = $ind1 . $ind2;
            $content[VALREF] = \$val;
            my $subnum = 0;
            while (@subs) {
                my ($subid, $subval);
                if (ref $subs[0]) {
                    my $sub = shift @subs;
                    die "subfield not a pair or pair-array?"
                        if ref $sub ne 'ARRAY';
                    ($subid, $subval) = @$sub;
                }
                elsif (@subs > 1) {
                    ($subid, $subval) = splice @subs, 0, 2;
                }
                else {
                    die "data field with half-subfield?";
                }
                $content[SUBS+$subnum] = [$subid, \$subval];
                $subnum++;
                $val .= "\x1f" . $subid . $subval;
            }
            @content[VALREF, IND1, IND2] = (\$val, $ind1, $ind2);
        }
        $self{'content'} = \@content;
    }
    else {
        die "field without a tag?";
    }
    return bless \%self, $cls;
}

sub is_deleted { @_ > 1 ? $_[0]{'content'}[DELETE] = $_[1] : $_[0]{'content'}[DELETE] }
sub is_dirty { @_ > 1 ? $_[0]{'is_dirty'} = $_[1] : $_[0]{'is_dirty'} }

sub value { goto &data }

sub data {
    my $self = shift;
    my $content = $self->{'content'};
    my ($tag, $valref) = @$content[TAG, VALREF];
    return $$valref if $tag lt '010' || !wantarray;
    return @$content[SUBS..$#$content];
}

sub tag { shift->{'content'}[TAG] }
sub set_tag {
    my ($self, $new_tag) = @_;
    my $old_tag = $self->{'content'}[TAG];
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
    die "undefined indicator $i"
        if !defined $v;
    my $n = length $v;
    die "invalid indicator length: $n"
        if $n != 1;
    $self->{'content'}[IND1+$i-1] = $v;
    $self->{'is_dirty'} = 1;
}

sub indicators {
    my $self = shift;
    my $content = $self->{'content'};
    if (!@_) {
        my @inds = @$content[IND1, IND2];
        return @inds if wantarray;
        return join('', @inds);
    }
    elsif (@_ == 1) {
        my $inds = shift;
        my $n = length $inds;
        die "too many indicators" if $n > 2;
        die "not enough indicators" if $n < 2;
        @$content[IND1, IND2] = split //, $inds;
    }
    elsif (@_ == 2) {
        @$content[IND1, IND2] = @_;
    }
    $self->{'is_dirty'} = 1;
    my @inds = @$content[IND1, IND2];
    return @inds if wantarray;
    return join('', @inds);
}

sub subfield {
    my ($self, $id) = @_;
    die if !defined $id;
    my $content = $self->{'content'};
    my @subs = map { $_->[SUB_ID] eq $id ? (${ $_->[SUB_VALREF] }) : () } @$content[SUBS..$#$content];
    return @subs if wantarray;
    return @subs ? shift @subs : undef;
}

sub subfields {
    my ($self) = @_;
    my $content = $self->{'content'};
    return @$content[SUBS..$#$content];
}

1;
