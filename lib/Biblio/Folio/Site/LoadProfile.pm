package Biblio::Folio::Site::LoadProfile;

sub init {
    my ($self) = @_;
    $self->SUPER::init;
    my $type = $self->{'type'};
    my $fields = $self->{'fields'} ||= {};
    foreach my $f (keys %$fields) {
        local $_ = $fields->{$f};
        my %field = (
            'field' => $f,
            'qualifier' => 'any',
            'required' => 0,
            'order' => 1<<15,
            'exact' => 1,
        );
        my $n = 0;
        while (/\S/) {
            if ($n++) {
                s/^\s*,\s+// or die "invalid field parameters: $f = $fields->{$f}";
            }
            if (s/^copy from (\w+)//) {
                $field{'copy_from'} = $1;
            }
            elsif (s/^(?:in|not )exact//) {
                $field{'exact'} = 0;
            }
            elsif (s/^default://) {
                my $dv = extract_value();
                die "invalid default: $_" if !defined $dv;
                $field{'default'} = $dv;
            }
            elsif (s/^([^:,\s]+)//) {
                my ($pk, $pv) = ($1, 1);
                if (s/^:\s*//) {
                    $pv = extract_value();
                    die "invalid value for $pk: $_" if !defined $pv;
                }
                # if ($pk =~ /^(lowest|highest)$/) {
                #     ($pk, $pv) = ('qualifier', _matchpoint_qualifier($pk));
                # }
                $field{$pk} = $pv;
            }
        }
        die "contradictory mode for $type: field $f both required and optional"
            if $field{'required'} && $field{'optional'};
        $fields->{$f} = \%field;
    }
    return $self;
}

sub extract_value {
    # This function operates (destructively) on $_
    my $v = extract_delimited(undef, q{"'}, '\s*');
    if (defined $v) {
        $v =~ /^(["'])(.*)$1$/ or die "wtf?";
        $v = $2;
    }
    elsif (s/^(true|false|null)//) {
        $v = $tok2const{$1};
    }
    elsif (s/^([0-9]+(?:\.[0-9]+)?)//) {
        $v = $1;
    }
    elsif (s/^(\w+)//) {
        $v = $1;
    }
    return $v;
}

1;

