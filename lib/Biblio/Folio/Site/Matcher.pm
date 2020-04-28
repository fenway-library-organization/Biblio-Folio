package Biblio::Folio::Site::Matcher;

use strict;
use warnings;

use Biblio::Folio::Util qw(_cql_value _cql_term _cql_or _cql_and);

use constant NO => 0;
use constant MAYBE => 1;
use constant YES => 2;

sub new {
    my $cls = shift;
    my $self = bless { @_ }, $cls;
    $self->init;
    return $self;
}

sub site { @_ > 1 ? $_[0]{'site'} = $_[1] : $_[0]{'site'} }
sub kind { @_ > 1 ? $_[0]{'kind'} = $_[1] : $_[0]{'kind'} }
sub profile { @_ > 1 ? $_[0]{'profile'} = $_[1] : $_[0]{'profile'} }
sub matrix { @_ > 1 ? $_[0]{'matrix'} = $_[1] : $_[0]{'matrix'} }
sub incoming { @_ > 1 ? $_[0]{'incoming'} = $_[1] : $_[0]{'incoming'} }
sub candidates { @_ > 1 ? $_[0]{'candidates'} = $_[1] : $_[0]{'candidates'} }
sub include_rejects { @_ > 1 ? $_[0]{'include_rejects'} = $_[1] : $_[0]{'include_rejects'} }

sub init {
    my ($self) = @_;
    my $profile = $self->profile;
    my @fields = $profile->fields;
    @$self{qw(fields matrix)} = (\@fields, $self->matrix_stub);
    $self->{'check_fields'} = { map { $_->{'is_check'} ? ($_->{'field'} => 1) : () } @fields };
    $self->{'matchpoints'} = { map { $_->{'is_matchpoint'} ? ($_->{'field'} => 1) : () } @fields };
    return $self;
}

sub check_fields {
    my ($self, $field) = @_;
    my $check_fields = $self->{'check_fields'};
    return $check_fields->{$field} if defined $field;
    return keys %$check_fields if wantarray;
    return $check_fields;
}

sub matchpoints {
    my ($self, $field) = @_;
    my $matchpoints = $self->{'matchpoints'};
    return $matchpoints->{$field} if defined $field;
    return keys %$matchpoints if wantarray;
    return $matchpoints;
}

sub match {
    my ($self, @records) = @_;
    my $kind = $self->kind;
    my $profile = $self->profile;
    $self->reset;
    $self->populate('incoming' => \@records);
    my $query = $self->make_cql_query;
    my @candidates = $self->site->objects($kind, 'query' => $query);
    $self->populate('candidates' => \@candidates);
    return $self->results;
}

sub populate {
    my ($self, $phase, $records) = @_;
    # $phase is 'incoming' or 'candidates'
    # @$records are the incoming records or the candidate matches
    my $matrix = $self->matrix;
    my $profile = $self->profile;
    my @results;
    $matrix->{$phase}{'records'} = $records;
    $matrix->{$phase}{'results'} = \@results;
    my $sets = $matrix->{'sets'} ||= {};
    foreach my $n (0..$#$records) {
        my $record = $records->[$n];
        my @fields = $profile->fields($record);
        my %result = (
            'record' => $record,
            'n' => $n,
            'fields' => \@fields,
            'matching' => {},
            'rejected' => {},
            # 'disposition' => undef,
        );
        $results[$n] = \%result;
        foreach my $field (@fields) {
            my ($f, $cqlv) = @$field{qw(field cqlvalue)};
            my $set = $sets->{$f}{$cqlv} ||= {
                %$field,
            };
            $set->{$phase}{$n} = 1;
        }
    }
    return $matrix;
}

sub reset {
    my ($self) = @_;
    $self->matrix($self->matrix_stub);
    return $self;
}

sub results {
    my ($self) = @_;
    my $matrix = $self->matrix;
    my $check_fields = $self->check_fields;
    my $matchpoints = $self->matchpoints;
    my @check_fields = keys %$check_fields;
    my @matchpoints = keys %$matchpoints;
    my ($incoming, $candidates, $sets) = @$matrix{qw(incoming candidates sets)};
    my ($irecords, $crecords) = map { $_->{'records'} } $incoming, $candidates;
    my ($iresults, $cresults) = map { $_->{'results'} } $incoming, $candidates;
    my @iall = (0..$#$irecords);
    my @call = (0..$#$crecords);
    my (@m, @unsatisfied);
    my %num_checks_passed;
    foreach my $i (@iall) {
        foreach my $c (@call) {
            my $k = $i . ':' . $c;
            $num_checks_passed{$k} = 0;
        }
    }
    foreach my $f (@check_fields) {
        my $cqlv2set = $sets->{$f} or next;
        my @cqlv = keys(%$cqlv2set) or next;
        my %pairs_passed;
        foreach my $cqlv (@cqlv) {
            my $set = $cqlv2set->{$cqlv};
            my $i2node = $set->{'incoming'};
            my $c2node = $set->{'candidates'};
            delete $cqlv2set->{$cqlv}, next if !$i2node;
            delete $cqlv2set->{$cqlv}, next if !$c2node;
            my @i = keys %$i2node or next;
            my @c = keys %$c2node or next;
            foreach my $i (@i) {
                foreach my $c (@c) {
                    my $k = $i . ':' . $c;
                    $pairs_passed{$k} = 1;
                    #$m->[$c] = {};
                }
            }
        }
        foreach my $k (keys %pairs_passed) {
            $num_checks_passed{$k}++;  # This pair of records match in this one check field
        }
    }
    my %match;
    my %rejected;
    my @match_pairs;
    my $must_pass = @check_fields;
    foreach my $f (@matchpoints) {
        my $v2set = $sets->{$f} or next;
        while (my ($v, $set) = each %$v2set) {
            my $i2node = $set->{'incoming'  } or next;
            my $c2node = $set->{'candidates'} or next;
            my @i = keys %$i2node or next;
            my @c = keys %$c2node or next;
            foreach my $i (@i) {
                #my $m = $match[$i] = [];
                foreach my $c (@c) {
                    my $k = $i . ':' . $c;
                    my $passed = $num_checks_passed{$k};
                    if ($passed == $must_pass) {
                        # Record how the pair matched
                        $match{$k}{$f}{$v} = 1;
                    }
                    elsif ($passed < $must_pass) {
                        # Record a rejected match
                        $rejected{$k}{$f}{$v} = 1;
                    }
                    else {
                        die "wtf?";
                    }
                }
            }
        }
    }
    my @result;
    my %incoming_results;
    foreach my $k (keys %match) {
        my ($i, $c) = split /:/, $k;
        my $by = $match{$k};
        my $rejected = $rejected{$k};
        my $ires = $iresults->[$i];
        my $irec = $irecords->[$i];
        #my $cres = $cresults->[$c];
        my $crec = $crecords->[$c];
        my $imat = $ires->{'matching'};
        my $irej = $ires->{'rejected'};
        %$imat = ( %$imat, %$by ) if $by;
        %$irej = ( %$irej, %$rejected ) if $rejected;
        my @by = keys %$imat;
        my @rej = keys %$irej;
        push @{ $ires->{'matches'} ||= [] }, { 'object' => $crec, 'by' => \@by, 'rejected' => \@rej };
        #push @{ $cres->{'matches'} ||= [] }, { 'object' => $irec, 'by' => $by, 'rejected' => $rejected };
        push @match_pairs, [$i, $c];
    }
    $matrix->{'match_pairs'} = \@match_pairs;
    if (wantarray) {
        return @{ $matrix->{'incoming'}{'results'} };
    }
    else {
        return $matrix;
    }
}

###     if (0) {
###         while (my ($f, $v2set) = each %$sets) {
###             my $chk = $check_fields->{$f};
###             my $mat = $matchpoints->{$f};
###             while (my ($v, $set) = each %$v2set) {
###                 my $i2node = $set->{'incoming'  } or next;
###                 my $c2node = $set->{'candidates'} or next;
###                 my @i = keys %$i2node;
###                 my @c = keys %$c2node;
###                 # We have one or more matches at a matchpoint, but it may or may not be enough
###                 foreach my $i (@i) {
###                     #my $m = $match[$i];
###                     foreach my $c (@c) {
###                         #$m->[$c]{$f}{$v} = $mat ? 1 : 0;
###                         #delete $unsatisfied[$i][$c]{$f} if $chk;
###                     }
###                 }
###             }
###         }
###         foreach my $i (@iall) {
###             #my $m = $match[$i];
###             #my $u = $unsatisfied[$i];
###             my $nmatch = 0;
###             foreach my $c (@call) {
###                 next if !keys %{ $m->[$c] };
###                 #my $unsatisfied = $u->[$c];
###                 #undef($m->[$c]), next if keys %$unsatisfied;
###                 my $ires = $iresults->[$i];
###                 my $irec = $irecords->[$i];
###                 my $cres = $cresults->[$c];
###                 my $crec = $crecords->[$c];
###                 my %matched_by = %{ $m->[$c] };
###                 delete @matched_by{ keys %$check_fields };
###                 if (keys %matched_by) {
###                     # XXX This is a hack -- we shouldn't have to check for keys %matched_by!
###                     push @{ $ires->{'matches'} ||= [] }, { 'user' => $crec, 'by' => \%matched_by };
###                     push @{ $cres->{'matches'} ||= [] }, { 'user' => $irec, 'by' => \%matched_by };
###                     push @match_pairs, [$i, $c];
###                 }
###             }
###         }
###     }

sub make_matching_criteria {
    my ($self, $records) = @_;
    my $profile = $self->profile;
    my $matrix = $self->matrix;
    my (%check, %matchpoint);
    @$matrix{qw(check_fields matchpoints)} = (\%check, \%matchpoint);
    foreach my $n (0..$#$records) {
        my $record = $records->[$n];
        my @fields = $profile->fields($record);
        my %result = (
            'record' => $record,
            'n' => $n,
            'fields' => \@fields,
            'matches' => [],
        );
        next if !@fields;  # Nothing to match on
        foreach my $field (@fields) {
            my ($f, $v) = @$field{qw(field value)};
            my $cqlv = _cql_value($v);
            if ($field->{'is_check'}) {
                $check{$f}{$cqlv}{$n} = $v;
            }
            elsif ($field->{'is_matchpoint'}) {
                $matchpoint{$f}{$cqlv}{$n} = $v;
            }
        }
    }
    return (\%check, \%matchpoint);
}

sub make_cql_query {
    my ($self) = @_;
    my $matrix = $self->matrix;
    my $records = $matrix->{'incoming'}{'records'}
        or die "nothing to search";
    my $profile = $self->profile;
    my ($check_fields, $matchpoints) = $self->make_matching_criteria($records);
    my $fields = $profile->fields;
    my (@check_terms, @matchpoint_terms);
    if (!$self->include_rejects) {
        foreach my $f (sort keys %$check_fields) {
            my $cqlv2n2v = $check_fields->{$f};
            my @cqlv = keys %$cqlv2n2v;
            next if !@cqlv;
            my $field = $fields->{$f};
            push @check_terms, _cql_term($f, \@cqlv, $field, 1);
        }
    }
    foreach my $f (sort keys %$matchpoints) {
        my $cqlv2n2v = $matchpoints->{$f};
        my @cqlv = keys %$cqlv2n2v;
        next if !@cqlv;
        my $field = $fields->{$f};
        push @matchpoint_terms, _cql_term($f, \@cqlv, $field, 1);
    }
    return if !@matchpoint_terms;
    my $query = _cql_or(@matchpoint_terms);
    $query = _cql_and(@check_terms, $query)
        if @check_terms;
    die "unbalanced parens" if $query =~ s/^\(// && $query !~ s/\)$//;
    return $query;
    # patronGroup==("foo" or "bar")
    # and
    # active==true
    # and
    # (
    # externalSystemId==("baz" or "qux" or ...)
    # or
    # username==("abc" or "def" or ...)
    # or
    # barcode=("1234" or "5678" or ...)
    # or
    # personal.email=(...)
    # )
}

sub matrix_stub {
    my ($self) = @_;
    return {};
    # These will be filled in by populate()
    #   'sets' => {},
    #   'incoming' => [],
    #   'candidates' => [],
    #   'results' => [],
}

sub sample_matrix {
    # Incoming: 5 records
    # Candidates: 4 records
    my (@inc, @can, @ires, @cres);
    return (
        'incoming' => {
            'records' => \@inc,
            'results' => \@ires,
        },
        'candidates' => {
            'records' => \@can,
            'results' => \@cres,
        },
        'checks' => {
            'active' => [
                [ undef, undef, undef, undef ],
                [ undef, undef, undef, undef ],
                [ undef, undef, undef, undef ],
                [ undef, undef, undef, undef ],
                [ undef, undef, undef, undef ],
            ],
            'patronGroup' => [
                [ undef, undef, undef, undef ],
                [ undef, undef, undef, undef ],
                [ undef, undef, undef, undef ],
                [ undef, undef, undef, undef ],
                [ undef, undef, undef, undef ],
            ],
        },
        'sets' => {
            'externalSystemId' => {
                '1234567' => {
                    'incoming' => { map { $_ => 1 } qw(4) },
                },
                '8901234' => {
                    'incoming' => { map { $_ => 1 } qw(1) },
                    'candidates' => { map { $_ => 1 } qw(3) },
                },
            },
            'active' => {
                'true' => {
                    'incoming' => { map { $_ => 1 } qw(0 1 2 3 4) },
                    'candidates' => { map { $_ => 1 } qw(0 1 2 3) },
                },
            },
            'username' => {
                'fishwick' => {
                    'incoming' => { map { $_ => 1 } qw(1) },
                    'candidates' => { map { $_ => 1 } qw(0) },
                },
                'noamchomsky' => {
                    'incoming' => { map { $_ => 1 } qw(3) },
                    'candidates' => { map { $_ => 1 } qw(4) },
                },
                #...
            },
            'patronGroup' => {
                '98d29529-1e0b-4e44-adb4-e4bef6504148' => {
                    'incoming' => { map { $_ => 1 } qw(0 1 3 4) },
                    'candidates' => { map { $_ => 1 } qw(0 1 2 3) },
                },
                '7783ccf9-1d0a-46e5-9d9b-6e061dca3a48' => {
                    'incoming' => { map { $_ => 1 } qw(2) },
                },
            },
            #...
        },
        'matches' => {
            '0' => {},
            '1' => { map { $_ => 1 } qw(3) },
            '2' => {},
            '3' => { map { $_ => 1 } qw(4) },
            '4' => {},
        },
        'match_pairs' => [
            [1, 3],
            [3, 4],
        ],
    );
}

1;

