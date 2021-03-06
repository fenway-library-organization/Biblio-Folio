package Biblio::Folio::Site::LoadProfile;

use strict;
use warnings;

use base qw(Biblio::Folio::Site::Profile);

use Biblio::Folio::Util qw(
    _tok2const
    _cql_value
    _cql_term
    _cql_and
    _cql_or
    _get_attribute_from_dotted
    $rx_const_token
);
use Text::Balanced qw(extract_delimited);

sub init {
    my ($self) = @_;
    $self->SUPER::init;
    my $kind = $self->{'kind'};
    my $fields = $self->{'fields'} ||= {};
    foreach my $f (keys %$fields) {
        local $_ = $fields->{$f};
        my %field = (
            'field' => $f,
            'key' => $f,
            'qualifier' => 'any',
            'is_matchpoint' => 0,
            'is_check' => 0,
            'is_required' => 0,
            'order' => 1<<15,
            'exact' => 1,
        );
        my $n = 0;
        while (/\S/) {
            if ($n++) {
                s/^\s*,\s+// or die "invalid field parameters: $f = $fields->{$f}";
            }
            if (s/^matchpoint//) {
                $field{'is_matchpoint'} = 1;
            }
            elsif (s/^check//) {
                $field{'is_check'} = 1;
            }
            elsif (s/^required//) {
                $field{'is_required'} = 1;
            }
            elsif (s/^optional//) {
                $field{'is_required'} = 0;
            }
            elsif (s/^from:(\w+)//) {
                $field{'copy_from'} = $1;
            }
            elsif (s/^(?:in|not )exact//) {
                $field{'exact'} = 0;
                $field{'key'} =~ s/^[~]?/~/;
            }
            elsif (s/^default://) {
                my $dv = _extract_value();
                die "invalid default in field spec: $f = $fields->{$f}" if !defined $dv;
                $field{'default'} = $dv;
            }
            elsif (s/^([^:,\s]+)//) {
                my ($pk, $pv) = ($1, 1);
                if (s/^:\s*//) {
                    $pv = _extract_value();
                    die "invalid value for $pk in field spec: $f = $fields->{$f}" if !defined $pv;
                }
                # if ($pk =~ /^(lowest|highest)$/) {
                #     ($pk, $pv) = ('qualifier', _matchpoint_qualifier($pk));
                # }
                $field{$pk} = $pv;
            }
            elsif (/\S/) {
                die "unparseable field spec: $f = $fields->{$f}";
            }
        }
        die "contradictory mode for $kind: field $f both required and optional: $fields->{$f}"
            if $field{'is_required'} && $field{'is_optional'};
        $fields->{$f} = \%field;
    }
    return $self;
}

sub fields {
    my ($self, $obj) = @_;
    my $fields = $self->{'fields'};
    if (!defined $obj) {
        return values %$fields if wantarray;
        return $fields;
    }
    my (@fields, %field);
    while (my ($f, $field) = each %$fields) {
        my $val = _get_attribute_from_dotted($obj, $field->{'from'} || $f);
        $val = $field->{'default'} if !defined $val;
        my @values = map { defined $_ ? $_ : $field->{'default'} } ref($val) eq 'ARRAY' ? @$val : ($val);
        foreach my $v (@values) {
            next if !defined $v || !length $v;
            my $cqlv = _cql_value($v);
            my $field = {
                %$field,
                'value' => $v,
                'cqlvalue' => $cqlv,
            };
            if (wantarray) {
                push @fields, $field;
            }
            else {
                $field{$f}{$cqlv} = $field;
            }
        }
    }
    return @fields if wantarray;
    return \%field;
}

sub tiebreakers {
    my ($self) = @_;
    my @fields = $self->fields;
    return sort { $b->{'tiebreaker'} <=> $a->{'tiebreaker'} }
           grep { $_->{'tiebreaker'} }
           @fields;
}

sub _extract_value {
    # This function operates (destructively) on $_
    _extract_string()
        //
    _extract_token()
        //
    _extract_literal()
}

sub _extract_string {
    # This function operates (destructively) on $_
    my $v = extract_delimited(undef, q{"'}, '\s*');
    return if !defined $v;
    $v =~ /^(["'])(.*)$1$/ or die "wtf?";
    return $2;
}

sub _extract_token {
    # This function operates (destructively) on $_
    return if !s/^($rx_const_token)//o;
    return _tok2const($1);
}

sub _extract_literal {
    # This function operates (destructively) on $_
    return $1 if s/^([0-9]+(?:\.[0-9]+)?)//;
    return if !s/^(\w+)//;
    return $1;
}

1;

