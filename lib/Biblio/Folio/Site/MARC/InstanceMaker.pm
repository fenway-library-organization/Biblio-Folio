package Biblio::Folio::Site::MARC::InstanceMaker;

use strict;
use warnings;

use Biblio::Folio::Site::MARC;
use Biblio::Folio::Util qw(_uuid _unique _tok2const _use_class);

my $singleton;

sub new {
    my $cls = shift;
    return $singleton if $singleton;
    $singleton = bless { @_ }, $cls;
    $singleton->init;
    return $singleton;
}

sub site { @_ > 1 ? $_[0]{'site'} = $_[1] : $_[0]{'site'} }

sub _reference_data { @_ > 1 ? $_[0]{'reference_data'} = $_[1] : $_[0]{'reference_data'} }
sub _repeat_subs { @_ > 1 ? $_[0]{'repeat_subs'} = $_[1] : $_[0]{'repeat_subs'} }
sub _bib_levels { @_ > 1 ? $_[0]{'bib_levels'} = $_[1] : $_[0]{'bib_levels'} }
sub _pub_roles { @_ > 1 ? $_[0]{'pub_roles'} = $_[1] : $_[0]{'pub_roles'} }
sub _mapping_rules { @_ > 1 ? $_[0]{'mapping_rules'} = $_[1] : $_[0]{'mapping_rules'} }
sub _field_types { @_ > 1 ? $_[0]{'field_types'} = $_[1] : $_[0]{'field_types'} }

sub init {
    my ($self) = @_;
    my $site = $self->{'site'}
        or die __PACKAGE__ . ' requires a site';
    my $mapping_rules = $self->{'mapping_rules'};
    if (!defined $mapping_rules) {
        ($self->{'mapping_rules'}) = $site->all('mapping_rules');
        $mapping_rules = $self->{'mapping_rules'};
    }
    $self->{'default_status'} = $site->instance_status('terms' => {'code' => 'batch'});
    my %repeat_sub;
    while (my ($tag, $rules) = each %$mapping_rules) {
        next if $tag =~ /^_/;
        foreach my $rule (@$rules) {
            $rule->{'entityPerRepeatedSubfield'}
                or next;
            my $entities = $rule->{'entity'}
                or next;
            foreach my $ent (@$entities) {
                next if $ent->{target} =~ /Id$/;
                push @{ $repeat_sub{$tag} ||= [] }, $ent->{subfield}[0];
            }
        }
    }
    $self->{'repeat_subs'} = \%repeat_sub;
    $self->{'bib_level'} = {
      'm' => 'Monograph',
      'i' => 'Integrating Resource',
      's' => 'Serial'
    };
    $self->{'relations'} = {
      '0' => 'Resource',
      '1' => 'Version of resource',
      '2' => 'Related resource',
      '3' => 'No information provided'
    };
    $self->{'pub_roles'} = {
      '0' => 'Production',
      '1' => 'Publication',
      '2' => 'Distribution',
      '3' => 'Manufacture',
      '4' => 'Copyright notice date'
    };
#$$ BEGIN marc2inst.pl
#$$ LINES 253-287
    $self->{'field_types'} = {
        id => 'string',
        hrid => 'string',
        source => 'string',
        title => 'string',
        indexTitle => 'string',
        alternativeTitles => 'array.object',
        editions => 'array',
        series => 'array',
        identifiers => 'array.object',
        contributors => 'array.object',
        subjects => 'array',
        classifications => 'array.object',
        publication => 'array.object',
        publicationFrequency => 'array',
        publicationRange => 'array',
        electronicAccess => 'array.object',
        instanceTypeId => 'string',
        instanceFormatIds => 'array',
        physicalDescriptions => 'array',
        languages => 'array',
        notes => 'array.object',
        modeOfIssuanceId => 'string',
        catalogedDate => 'string',
        previouslyHeld => 'boolean',
        staffSuppress => 'boolean',
        discoverySuppress => 'boolean',
        statisticalCodeIds => 'array',
        sourceRecordFormat => 'string',
        statusId => 'string',
        statusUpdatedDate => 'string',
        tags => 'object',
        holdingsRecords2 => 'array.object',
        natureOfContentTermIds => 'array.string'
    };
#$$ END marc2inst.pl
    my %refdata;
    my %data_class = Biblio::Folio::Classes->data_classes;
    foreach my $name (sort keys %data_class) {
        my $cls = $data_class{$name};
        _use_class($cls);
        my @objects = $cls->_all($site);
        $refdata{$name} = \@objects;
    }
    $self->{'reference_data'} = \%refdata;  # TODO
}

sub make {
    my ($self, $marc) = @_;
    my $r = ref $marc;
    if ($r eq '' || $r eq 'SCALAR') {
        $marc = Biblio::Folio::Site::MARC->new($marc);
    }
    elsif ($r ne 'Biblio::Folio::Site::MARC') {
        die "wtf?";
    }
    my $rec = $self->_empty_instance;
    my $refdata = $self->_reference_data;
    my $repeat_subs = $self->_repeat_subs;
    my $blvl = $self->_bib_levels;
    my $pub_roles = $self->_pub_roles;
    my $mapping_rules = $self->_mapping_rules;
    my $ftypes = $self->_field_types;
#$$ BEGIN marc2inst.pl
#$$ LINES 360-444
    my $ldr = $marc->leader();
    my $blevel = substr($ldr, 7, 1);
    my $mode_name = $blvl->{$blevel} || 'Other';
    $rec->{modeOfIssuanceId} = $refdata->{issuanceModes}->{$mode_name};
    my @marc_fields = $marc->fields;
MARC_FIELD:
    foreach my $field (@marc_fields) {
        my $tag = $field->tag;
        # Let's determine if a subfield is repeatable, if so create append separate marc fields for each subfield;
        my %repeatable_for_tag = map { $_ => 1 } @{ $repeat_subs->{$tag} };
        my @repeatable_for_tag = sort keys %repeatable_for_tag;
        foreach my $repeatable_subfield_code (@repeatable_for_tag) {
            my @subfields = $field->subfields($repeatable_subfield_code);
            if (@subfields > 1) {
                my $new_field;
                my $i = 0;
                my @subs = $field->subfields();
                foreach (@subs) {
                    my ($code, $sdata) = @$_;
                    if ($code eq $repeatable_subfield_code) {
                        $new_field = MARC::Field->new($tag, $field->{_ind1}, $field->{_ind2}, $code => $sdata);
                    }
                    elsif ($new_field->{_tag}) {
                        $new_field->add_subfields($code => $sdata );
                    }
                    $i++;
                    my ($ncode) = @{ $subs[$i] };
                    push @marc_fields, $new_field if ($repeatable_for_tag{$ncode} && $new_field->{_tag}) || $ncode eq undef;
                }
                next MARC_FIELD;
            }
        }
        my $fld_conf = $mapping_rules->{$tag};
        my @entities;
        if ($fld_conf) {
            if ($fld_conf->[0]->{entity}) {
                foreach (@{ $fld_conf }) {
                    if ($_->{entity}) {
                        push @entities, $_->{entity};
                    }
                }
            } else {
                @entities = $fld_conf;
            }
            foreach (@entities) {
                my @entity = @$_;
                my $data_obj = {};
                foreach (@entity) {
                    my @required = @{ $_->{requiredSubfield} };
                    if ($required[0] && !$field->subfield($required[0])) {
                        next;
                    }
                    my @targ = split /\./, $_->{target};
                    my $flavor = $ftypes->{$targ[0]};
                    my $data = process_entity($field, $_);
                    next unless $data;
                    if ($flavor eq 'array') {
                        if ($_->{subFieldSplit}) { # subFieldSplit is only used for one field, 041, which may have a lang string like engfreger.
                            my $val = $_->{subFieldSplit}->{value};
                            my @splitdata = $data =~ /(\w{$val})/g;
                            push @{ $rec->{$targ[0]} }, @splitdata;
                        } else {
                            push @{ $rec->{$targ[0]} }, $data;
                        }
                    } elsif ($flavor eq 'array.object') {
                        $data_obj->{$targ[0]}->{$targ[1]} = $data;
                    } elsif ($flavor eq 'object') {
                    } elsif ($flavor eq 'boolean') {
                    } else {
                        $rec->{$targ[0]} = $data;
                    }
                }
                foreach (keys %$data_obj) {
                    if ($ftypes->{$_} eq 'array.object') {
                        push @{ $rec->{$_} }, $data_obj->{$_};
                    }
                }
            }
        }
    }
    # Do some some record checking and cleaning
    $rec->{subjects} = dedupe(@{ $rec->{subjects} });
    $rec->{languages} = dedupe(@{ $rec->{languages} });
    $rec->{series} = dedupe(@{ $rec->{series} });
    #$$ END marc2inst.pl
}

sub _empty_instance {
    my ($self) = @_;
    my $false = _tok2const('false');
    return {
        'id' => _uuid(),
        'alternativeTitles' => [],
        'editions' => [],
        'series' => [],
        'identifiers' => [],
        'contributors' => [],
        'subjects' => [],
        'classifications' => [],
        'publication' => [],
        'publicationFrequency' => [],
        'publicationRange' => [],
        'electronicAccess' => [],
        'instanceFormatIds' => [],
        'physicalDescriptions' => [],
        'languages' => [],
        'notes' => [],
        'staffSuppress' => $false,
        'discoverySuppress' => $false,
        'statisticalCodeIds' => [],
        'tags' => {},
        'holdingsRecords2' => [],
        'natureOfContentTermIds' => [],
        'statusId' => $self->{'default_status'},
    };
}

sub dedupe {
    return [ _unique(@_) ];
}

1;
