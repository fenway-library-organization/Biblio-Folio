package Biblio::Folio::Object::User;

use strict;
use warnings;

sub _formatter_as_text {
    my ($cls, %defarg) = @_;
    return sub {
        my $self = shift;
        my %arg = ( %defarg, @_ );
        my $site = $self->site;
        my $patron_group_mapper = sub { $site->expand_uuid('patron_group' => shift) };
        my $matches_formatter = $cls->_formatter_as_text(%defarg);
        return (
            $self->_format_property('id'),
            $self->_format_property('hrid'),
            $self->_format_property('patronGroup', $patron_group_mapper),
            $self->_format_property('username'),
            $self->_format_property('externalSystemId'),
            $self->_format_property('personal.lastName'),
            $self->_format_property('personal.firstName'),
            ['file',      $self->{'_file'}],
            ['rowNumber', $self->{'_n'}],
            ['rawData',   $self->{'_raw'}],
            ['matches',   [$self->{'_matches'}], $matches_formatter],
        );
    };
}

sub _format_property {
    my ($self, $k, $vcode) = @_;
    my $v = $self->{$k};
    return if !defined $v;
    return [$k, $v] if !defined $vcode;
    return [$k, $vcode->($v)];
}
