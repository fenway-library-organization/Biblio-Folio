package Biblio::Folio::Site::MARC;

use strict;
use warnings;

use Biblio::Folio::Util qw(_optional _json_decode);
use MARC::Loop qw(marcloop marcparse marcfield marcbuild TAG DELETE VALREF IND1 IND2 SUBS);
use Scalar::Util qw(blessed);
use Encode qw(encode decode :fallback_all);

require bytes;
# use bytes;  NO NO NO NO NO!!!!!!

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

sub init {
    my ($self) = @_;
    $self->{'is_parsed'} = 0;
    $self->{'is_dirty'} = 0;
    $self->{'marcjson'} = $self->_marcjson($self->{'marcjson'})
        or $self->{'marcref'} = $self->_marcref($self->{'marcref'});
    delete $self->{'errors'};
    return $self;
}

sub stub {
    # Construct a stub suitable for use to delete records from discovery
    my ($proto, %arg) = @_;
    my ($leader, $status, $instance) = @arg{qw(leader status instance)};
    # There's really no need to set $cls -- we could just call $proto->new
    # below and the result would be exactly the same -- but it helps make it
    # clearer what's going on
    my $cls;
    if (ref $proto) {
        # Called on an object of this class
        my $self = $proto;  # Just for clarity
        $leader ||= $self->leader;
        $instance ||= $self->instance;
        $cls = ref $proto;
    }
    else {
        # Called as a class method
        $leader ||= _default_leader();
        $cls = $proto;
    }
    if (defined $status) {
        die "invalid status: $status" if $status !~ /^[a-z]$/;
        substr($leader, 5, 1) = $status;
    }
    my $hrid = $arg{'hrid'} || ($instance ? $instance->hrid : undef);
    my @fields = defined $hrid ? (marcfield('001', $hrid)) : ();
    my $marc = marcbuild($leader, \@fields);
    return $cls->new(\$marc);
}

sub leader {
    my $self = shift;
    if (@_) {
        $self->{'is_dirty'} = 1;
        return $self->{'leader'} = shift;
    }
    elsif (!$self->{'is_parsed'}) {
        $self->parse;
    }
    return $self->{'leader'};
}

sub fields {
    my $self = shift;
    if (@_) {
        my $fields = shift;
        $_->{'record'} = $self for @$fields;
        $self->{'is_dirty'} = 1;
        return $self->{'fields'} = $fields;
    }
    elsif (!$self->{'is_parsed'}) {
        $self->parse;
    }
    return @{ $self->{'fields'} } if wantarray;
    return $self->{'fields'};
}

sub status {
    my $self = shift;
    $self->parse if !$self->{'is_parsed'};
    if (@_) {
        $self->{'is_dirty'} = 1;
        return substr($self->{'leader'}, 5, 1) = substr(shift, 0, 1);
    }
    else {
        return substr($self->{'leader'}, 5, 1);
    }
}

sub marcref {
    my $self = shift;
    if (@_) {
        $self->{'is_parsed'} = 0;
        $self->{'is_dirty'} = 1;
        delete @$self{qw(leader fields)};
        return $self->{'marcref'} = shift;
    }
    else {
        my $marcref = $self->{'marcref'};
        return $marcref if defined $marcref;
        my $marc;
        $marcref = \$marc;
        if ($self->{'is_parsed'}) {
            $$marcref = $self->as_marc21;
            return $marcref;
        }
        else {
            die "no MARC data to reference";
        }
    }
}

sub is_valid {
    my ($self) = @_;
    return 0 if $self->{'errors'};
    return 1 if $self->{'is_parsed'};
    $self->parse;
    return $self->{'errors'} ? 0 : 1;
}

sub errors {
    my ($self) = @_;
    return if $self->is_valid;
    my $errors = $self->{'errors'};
    return @$errors if wantarray;
    return $errors;
}

# sub leader { @_ > 1 ? $_[0]{'leader'} = $_[1] : $_[0]->parse->{'leader'} || _default_leader() }
# sub fields { @_ > 1 ? $_[0]{'fields'} = $_[1] : $_[0]->parse->{'fields'} || [] }
# sub status { @_ > 1 ? substr($_[0]{'leader'},5,1) = $_[1] : substr($_[0]->parse->{'leader'},5,1) }

# sub marcref { @_ > 1 ? $_[0]{'marcref'} = $_[0]->_marcref($_[1]) : $_[0]{'marcref'} }

sub instance { @_ > 1 ? $_[0]{'instance'} = $_[1] : $_[0]{'instance'} }
sub source_record { @_ > 1 ? $_[0]{'source_record'} = $_[1] : $_[0]{'source_record'} }

sub is_parsed { @_ > 1 ? $_[0]{'is_parsed'} = $_[1] : $_[0]{'is_parsed'} }
sub is_dirty { @_ > 1 ? $_[0]{'is_dirty'} = $_[1] : $_[0]{'is_dirty'} }

sub _marcref {
    # Return a reference to a MARC record *as a string of bytes* (see below)
    my ($self, $marcref) = @_;
    return if !defined $marcref;
    my $r = ref $marcref;
    if ($r eq '') {
        my $marc = $marcref;
        $marcref = \$marc;
    }
    elsif ($r ne 'SCALAR') {
        die "can't make a scalar reference out of a(n) $r";
    }
    # ----------------------------------------------------------------------
    # In order for the caller to parse the MARC record that we return, it must
    # be a reference to a string of bytes (a.k.a. "octets"), not characters.
    # Not only is the record length (in the first five bytes of the leader)
    # specified in bytes, but all of the offsets and lengths in the directory
    # (the bytes after the leader but before the first occurrence of "\x1e")
    # are expressed in terms of bytes, not characters.  If you misinterpret
    # these when parsing, you may get garbage back.
    # ----------------------------------------------------------------------
    my $len = length $$marcref;
    my $len_in_bytes = bytes::length($$marcref);
    my $reclen = 0 + substr($$marcref, 0, 5);
    if ($reclen == $len_in_bytes) {
        if ($len != $len_in_bytes) {
            # Someone gave us characters
            utf8::encode($$marcref);
            $len = length $$marcref;
        }
    }
    elsif ($reclen == $len) {
        utf8::encode($$marcref);
        $len = length $$marcref;
        substr($$marcref, 0, 5) = sprintf('%05d', $len);
    }
    return $marcref;
    if (0) {
            # For some reason, $$marcref is a string of characters -- convert it to
            # a string of bytes.  We work with a copy because, otherwise, if
            # $$marcref were invalid in some fundamental way we would leave it
            # undefined.
                # Worse news. FOLIO actually stores source records with directory
                # entries whose offsets and lengths are expressed as characters,
                # not bytes.  We have to parse the whole record, tell Perl it's all
                # bytes, and then rebuild the record.
        my $is_utf8 = utf8::is_utf8($$marcref);
        my $reclen_is_right = (length($$marcref) == 0 + substr($$marcref, 0, 5));
        my $all_ascii = ($$marcref !~ /[^\x00-\x7f]/);
        if ($all_ascii) {
            if ($is_utf8) {
                utf8::downgrade($$marcref);  # This may die!
            }
            substr($$marcref, 0, 5) = sprintf('%05d', length $$marcref);
            return $marcref;
        }
        elsif (!$is_utf8) {
            # Make it characters
            utf8::decode($$marcref) or die "uh-oh!";
        }
        my ($ok, $leader, $fields);
        eval {
            substr($$marcref, 0, 5) = sprintf('%05d', length $$marcref);
            ($leader, $fields) = marcparse($marcref, 'error' => sub {
                my ($msg) = @_;
                push @{ $self->{'errors'} ||= [] }, $msg;
            });
            foreach (@$fields) {
                my $valref = $_->[VALREF];
                $$valref = encode('UTF-8', $$valref);
            }
            my $marc = marcbuild($leader, $fields);
            $marcref = \$marc;
            $ok = 1;
        };
        if (!$ok) {
            die "worse MARC record";
        }
    }
    return $marcref;
}
### if ($$marcref =~ /[^\x00-\x7f]/) {
###     # (1) 
###     my ($leader, $fields);
###     eval {
###         my $reclen = substr($$marcref, 0, 5);
###         
###     if (!$is_utf8) {
###         $reclen = utf8::upgrade($$marcref);
###     }
###     if ($reclen != length $$marcref) {
###     substr($$marcref, 0, 5) = sprintf('%05d', length $$marcref);
###     my ($leader, $fields);
###     foreach my $encode (1, 0) {
###         my ($leader, $fields);
###         eval {
###             ($leader, $fields) = marcparse($marcref);
###             if ($encode) {
###                 foreach (@$fields) {
###                     my $valref = $_->[VALREF];
###                     $$valref = encode('UTF-8', $$valref);
###                 }
###             }
###         };
###         last if defined $leader;
###     }
###     die "unparseable MARC data" if !defined $leader;
###     my $marc = marcbuild($leader, $fields);
###     #substr($marc, 0, 5) = sprintf('%05d', length $marc);
###     $marcref = \$marc;
### }
### elsif (utf8::is_utf8($$marcref)) {
###     # No special characters, but Perl sees it as a string of characters
###     my $marc = eval { encode('UTF-8', $$marcref, LEAVE_SRC|DIE_ON_ERR) };
###     if (!defined $marc) {
###         my ($err) = split /\n/, $@;
###         die "MARC data can't be converted from characters to octets: $err\n";
###     }
###     $marcref = \$marc;
### }
### #elsif (0) {
### #    # XXX Bad!  Don't do this!!!  See above.
### #    $$marcref = eval { decode('UTF-8', $$marcref, LEAVE_SRC|DIE_ON_ERR) };
### #    die "MARC data can't be decoded as UTF-8\n" if !defined $$marcref;
### #}
### if (1) {
###     # XXX Debugging code
###     my $length_in_octets = sprintf('%05d', length $$marcref);
###     if ($length_in_octets ne substr($$marcref, 0, 5)) {
###         substr($$marcref, 0, 5) = $length_in_octets;
###     }
### }
### return $marcref;
###}

sub _marcjson {
    # Return a reference to a MARC record as a parsed MARCJSON hash
    my ($self, $marcjson) = @_;
    return if !defined $marcjson;
    my $r = ref $marcjson;
    if ($r eq '') {
        # $self->_marcjson($str);
        $marcjson = _json_decode($marcjson);
        $r = ref $marcjson;
    }
    elsif ($r eq 'SCALAR') {
        # $self->_marcjson(\$str);
        $marcjson = _json_decode($$marcjson);
        $r = ref $marcjson;
    }
    if (eval { 1 + keys(%$marcjson) }) {
        # $self->_marcjson(_json_decode($str));
    }
    else {
        die "can't make a MARCJSON hash out of a(n) $r";
    }
    my %key = map { $_ => 1 } keys %$marcjson;
    die "MARCJSON without a leader" if !delete $key{'leader'};
    die "MARCJSON without fields" if !delete $key{'fields'};
    if (keys %key) {
        my $keys = join(', ', sort keys %key);
        die "MARCJSON with extraneous elements: $keys";
    }
    return $marcjson;
}

sub parse {
    my ($self, %what) = @_;
    my ($marcjson, $marcref);
    return $self if $self->{'is_parsed'};
    if (defined $what{'marcjson'}) {
        $marcjson = $self->{'marcjson'} = $self->_marcjson($what{'marcjson'});
    }
    elsif (defined $what{'marcref'}) {
        $marcref = $self->{'marcref'} = $self->_marcref($what{'marcref'});
    }
    else {
        $marcjson = $self->{'marcjson'}
            or $marcref = $self->{'marcref'}
            or die "nothing to parse: $self";
    }
    my $ok;
    eval {
        delete $self->{'errors'};
        if ($marcjson) {
            $self->{'leader'} = $marcjson->{'leader'};
            my $fields = $marcjson->{'fields'};
            $self->{'fields'} = [ map {
                _make_field_from_marcjson($self, $_)
            } @$fields ];
        }
        elsif ($marcref) {
            my ($leader, $fields) = marcparse($marcref, 'error' => sub {
                my ($msg) = @_;
                push @{ $self->{'errors'} ||= [] }, $msg;
            });
            $self->{'leader'} = $leader;
            $self->{'fields'} = [ map {
                _make_field($self, $_)
            } @$fields ];
        }
        $ok = $self->{'is_parsed'} = 1;
    };
    return $self if $ok;
}

sub field {
    # @fields = $marc->field($tag_or_coderef_or_regexp);
    # $field = $marc->field($tag_or_coderef_or_regexp);
    my ($self, $what, $first) = @_;
    $self->parse if !$self->{'is_parsed'};
    my @fields = grep { !$_->{'content'}[DELETE] } @{ $self->{'fields'} };
    my $ref = ref $what;
    if ($ref eq '') {
        $what = qr/$what/;
        $ref = ref $what;
    }
    if ($ref eq 'CODE') {
        @fields = grep { $what->() } @fields;
    }
    elsif ($ref eq 'Regexp') {
        # Possible ways of specifying fields to match:
        #   $marc->field(qr/6../);      Tag with wildcards
        #   $marc->field(qr/648/);      Tag alone
        #   $marc->field(qr/648 0/);    Tag with indicators
        #   $marc->field(qr/648#0/);    Ditto, with "#" for " "
        @fields = grep {
            my $matches;  # What we return
            my $field = $_;
            my $content = $field->{'content'};
            my $tag = $content->[TAG];
            my $inds = $tag ge '010' ? $content->[IND1] . $content->[IND2] : '';
            (my $inds2 = $inds) =~ tr/ /#/;
            foreach ($tag, $tag . $inds, $tag . $inds2) {
                next if !/^$what/;
                $matches = 1;
                last;
            }
            $matches;
        } @fields;
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

sub subfield {
    my ($self, $what, $sub) = @_;
    my ($field) = $self->field($what);  # First one only, for compatibility with MARC::Record
    return if !defined $field;
    return $field->subfield($sub);
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
        my ($leader, $fields) = marcparse($marcref, 'error' => sub {
            my ($msg) = @_;
            push @{ $self->{'errors'} ||= [] }, $msg;
        });
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

sub as_marc21 {
    my ($self) = @_;
    my $leader = $self->leader;
    my @fields = $self->fields;
    my $dirty = $self->is_dirty;
    if (!$dirty) {
        foreach my $field (@fields) {
            $dirty = 1 if $field->{'is_dirty'};
        }
        if (!$dirty) {
            my $marcref = $self->{'marcref'};
            return $$marcref if defined $marcref;
        }
    }
    return marcbuild($leader, [ map { $_->{'content'} } @fields ]);
}

sub as_marcjson {
    my ($self) = @_;
    my $leader = $self->leader;
    my @fields = $self->fields;
    my $dirty = $self->is_dirty;
    if (!$dirty) {
        foreach my $field (@fields) {
            $dirty = 1 if $field->{'is_dirty'};
        }
        if (!$dirty) {
            my $marcjson = $self->{'marcjson'};
            return $marcjson if $marcjson;
        }
    }
    return $self->{'marcjson'} = {
        'leader' => $leader,
        'fields' => [ map { _field_to_marcjson_field($_) } @fields ],
    }
}

sub _default_leader {
    return '00000cam a2200000 a 4500';
}

sub _field_to_marcjson_field {
    my ($field) = @_;
    my $content = $field->{'content'};
    return if $content->[DELETE];
    my $tag = $content->[TAG];
    if ($tag lt '010') {
        return +{
            $tag => ${ $content->[VALREF] },
        };
    }
    else {
        my @subs = @$content[SUBS..$#$content];
        return +{
            $tag => {
                'ind1' => $content->[IND1],
                'ind2' => $content->[IND2],
                'subfields' => [
                    map  {
                        my ($k, $v) = @$_[0,1];
                        +{ $k => $$v };
                    }
                    grep { !$_->[DELETE] } @subs
                ],
            },
        };
    }
}

sub new_field {
    my $self = shift;
    return _make_field($self, @_);
}

sub insert_field {
    my ($self, $field, %where) = @_;
    my $tag = $field->tag;
    $field->record($self);
    my $fields = $self->fields;
    if (!keys %where) {
        @$fields = (
            ( grep { $_->tag le $tag } @$fields ),
            $field,
            ( grep { $_->tag gt $tag } @$fields ),
        );
    }
    else {
        die "not implemented";
    }
    $self->is_dirty(1);
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

sub _make_field_from_marcjson {
    my ($self, $field) = @_;
    my ($tag, $data) = %$field;
    if ($tag lt '010') {
        return _make_field($self, $tag, $data);
    }
    else {
        my ($ind1, $ind2) = @$data{qw(ind1 ind2)};
        my @subs = map { %$_ } @{ $data->{'subfields'} };
        #? my @subs = map {
        #?     my ($k, $v) = %$_;
        #?     ($k => \$v)
        #? } @{ $data->{'subfields'} };
        return _make_field($self, $tag, $ind1, $ind2, @subs);
    }
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
            elsif ($r->can('tag')) {
                push @conditions, sub { shift() eq $what };
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
    $self->is_dirty(1) if $n;
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
    my ($holdings, $spell_out_locations, $copy_electronic_access, $add_items) = @arg{qw(holdings spell_out_locations copy_electronic_access add_items)};
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
        if ($copy_electronic_access) {
            my @elec = @{ $holding->electronicAccess || [] };
            if (@elec) {
                # Copy links from holdings to bib (unless they're already there)
                my %uri_in_bib = map {
                    my $u = $_->subfield('u');
                    defined $u ? ($u => 1) : ()
                } $self->field('856');
                foreach my $elec (@elec) {
                    # {
                    #     "linkText" : "Access E-Book",
                    #     "publicNote" : "",
                    #     "relationshipId" : "f5d0068e-6272-458e-8a81-b85e7b9a14aa",
                    #     "uri" : "https://ezproxy.simmons.edu/login?url=http://search.ebscohost.com/login.aspx?direct=true&scope=site&db=nlebk&db=nlabk&AN=6436",
                    # }
                    my $uri = $elec->{'uri'};
                    next if !defined $uri || $uri_in_bib{$uri};
                    my $rel = $elec->{'relationshipId'};  # TODO Don't just ignore this!
                    my ($link_text, $public_note) = @$elec{qw(linkText publicNote)};
                    undef $public_note if defined $public_note && $public_note !~ /\S/;  # Disregard blank notes 
                    push @add, _make_field($self,
                        '856', '4', '0',  # XXX Hard-coded indicators
                        'u' => $uri,
                        _optional('y' => $link_text),
                        _optional('z' => $public_note),
                    );
                    $uri_in_bib{$uri} = 1;
                }
            }
        }
        $num_holdings++;
        if ($add_items) {
            my @item_fields = _make_item_fields($self, $holding);
            push @add, @item_fields;
            $num_items += @item_fields;
        }
    }
    if (@add) {
        $self->delete_fields('852');
        $self->add_fields(@add);
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

sub is_deleted {
    my $self = shift;
    my $del = !!$self->{'content'}[DELETE];
    return $del if !@_;
    my $new_del = !!shift;
    return $new_del if $new_del eq $del;
    $self->{'content'}[DELETE] = $new_del;
    $self->{'is_dirty'} = 1;
}

sub record { @_ > 1 ? $_[0]{'record'} = $_[1] : $_[0]{'record'} }
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

sub add_subfields {
    my $self = shift;
    my $content = $self->{'content'};
    return undef if @_ % 2;  # For compatibility with MARC::Field, sadly
    push @$content, @_;
    $self->{'is_dirty'} = 1;
    return @_ / 2;
}

1;
