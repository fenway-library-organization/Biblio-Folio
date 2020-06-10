package Biblio::Folio::Object::Instance;

use strict;
use warnings;

use Biblio::Folio::Site::MARC;

# sub _obj_uri { '/instance-storage/instances/%s' }

sub marcref {
    my ($self, $what) = @_;
    my $r = ref $what;
    my $marcref;
    if (!defined $what) {
        return $self->{'_marcref'} if $self->{'_marcref'};
        die "not implemented";
        my $site = $self->site;
        my $id = $self->{'id'};
    }
    elsif ($r eq 'SCALAR') {
        return $self->{'_marcref'} = $what;
    }
    else {
        die "not implemented";
    }
}

sub holdings {
    my ($self, $what) = @_;
    my $holdings;
    my $r = ref $what;
    if (!defined $what) {
        $holdings = $self->{'_holdings'};
        if (!$holdings) {
            my $site = $self->site;
            my $id = $self->{'id'};
            $holdings = [$site->object('holdings_record', 'query' => "instanceId==$id")];
        }
    }
    elsif (!$r) {
        my $site = $self->site;
        $holdings = [$site->object('holdings_record', 'query' => $what)];
    }
    elsif ($r eq 'ARRAY') {
        $holdings = $what;
    }
    else {
        my $site = $self->site;
        $holdings = [$site->object('holdings_record', %$what)];
    }
    $_->{'_instance'} = $self for @$holdings;
    $self->{'_holdings'} = $holdings;
    return wantarray ? @$holdings : $holdings;
}

sub from_marcref {
    my ($self, $marcref) = @_;
    my $marc = Biblio::Folio::Object::MARC->new('marcref' => $marcref)->parse;
    my $maker = Biblio::Folio::Site::MARC::InstanceMaker->new('site' => $self->site);
    return $maker->make($marcref);
}

sub export_marc {
    my ($self, %arg) = @_;
    my ($marcref, $holdings) = delete @arg{qw(marcref holdings)};
    $marcref ||= $self->marcref;
    $holdings ||= $self->holdings if $arg{'with_holdings'} || $self->{'_holdings'};
    my $marc = Biblio::Folio::Site::MARC->new('marcref' => $marcref);
    $marc->parse;
    $marc->garnish('instance' => $self);
    if (@$holdings) {
        $marc->add_holdings(
            'holdings' => $holdings,
            'spell_out_locations' => $arg{'spell_out_locations'},
        );
    }
    else {
        $marc->delete_holdings;
    }
    return $marc;
}

1;
