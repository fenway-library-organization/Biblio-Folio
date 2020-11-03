#!/usr/bin/perl

use strict;
use warnings;

use Test::More;

plan tests => 5;

use_ok('Biblio::Folio::Util', qw(_parse_uuid_range _uuid_predecessor _uuid_successor));

my %range = (
    '3'     => '30000000-0000-0000-0000-000000000000:3fffffff-ffff-ffff-ffff-ffffffffffff',
    '[3,4)' => '30000000-0000-0000-0000-000000000000:3fffffff-ffff-ffff-ffff-ffffffffffff',
    '(2,4)' => '30000000-0000-0000-0000-000000000000:3fffffff-ffff-ffff-ffff-ffffffffffff',
);
my @uuids = qw(
    000009fa-a998-11ea-8da7-1466fadbd8b9
    000009fa-a998-11ea-8da7-1466fadbd8ba
    000009fa-a998-11ea-8da7-1466fadbd8bb
);

is(_uuid_successor($uuids[0]), $uuids[1], 'increment UUID (1)');
is(_uuid_successor($uuids[1]), $uuids[2], 'increment UUID (2)');

is(_uuid_predecessor($uuids[1]), $uuids[0], 'decrement UUID (1)');
is(_uuid_predecessor($uuids[2]), $uuids[1], 'decrement UUID (2)');

