package Biblio::Folio::Object::Instance;

use strict;
use warnings;

use Biblio::Folio::Site::MARC;
use Scalar::Util qw(weaken);

# sub _obj_uri { '/instance-storage/instances/%s' }

sub errors {
    my $self = shift;
    my $err = $self->{'_errors'} ||= [];
    if (@_) {
        push @$err, @_;
        return $self;
    }
    else {
        return @$err if wantarray;
        return $err;
    }
}

sub source_record {
    my ($self) = @_;
    my ($source_record) = $self->site->object('source_record', 'id' => $self->{'id'}, 'uri' => '/source-storage/records/%s/formatted?idType=INSTANCE');
    return $source_record;
}

sub marcjson {
    my ($self, $what) = @_;
    my $r = ref $what;
    my $marcjson;
    if (!defined $what) {
        return $self->{'_marcjson'} if $self->{'_marcjson'};
        die "not implemented";
    }
    elsif ($r eq 'HASH') {
        return $self->{'_marcjson'} = $what;
    }
    else {
        die "not implemented";
    }
}

sub marcref {
    my ($self, $what) = @_;
    my $r = ref $what;
    my $marcref;
    if (!defined $what) {
        return $self->{'_marcref'} if $self->{'_marcref'};
        die "not implemented";
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
            if ($site) {
                my $id = $self->{'id'};
                $holdings = [$site->object('holdings_record', 'query' => "instanceId==$id")];
            }
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
    foreach (@$holdings) {
        $_->{'_instance'} = $self;
        weaken($_->{'_instance'});  # XXX Kind of a hack
    }
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
    my $marc = $arg{'marc'};
    if (!$marc) {
        my $marcjson = $arg{'marcjson'} || $self->{'_marcjson'};
        my $marcref = $arg{'marcref'} || $self->{'_marcref'};
        if ($marcjson) {
            $marc = Biblio::Folio::Site::MARC->new('marcjson' => $marcjson);
        }
        elsif ($marcref) {
            $marc = Biblio::Folio::Site::MARC->new('marcref' => $marcref);
        }
        else {
            die "can't make up a MARC record";
        }
    }
    my $ok;
    eval {
        $marc->parse or die "can't parse";
        $marc->garnish('instance' => $self) or die "can't garnish";
        if ($arg{'with_holdings'}) {
            my $holdings = $arg{'holdings'} ? $self->holdings($arg{'holdings'}) : $self->holdings;
            if ($holdings && @$holdings) {
                $marc->add_holdings(
                    'holdings' => $holdings,
                    'spell_out_locations' => $arg{'spell_out_locations'},
                    'copy_electronic_access' => $arg{'copy_electronic_access'},
                );
            }
        }
        else {
            $marc->delete_holdings;
        }
        $ok = 1;
    };
    return $marc if $ok;
    push @{ $self->errors }, $@ || "can't make exportable MARC record", $marc->errors;
    return;
}

1;
