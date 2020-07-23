#!/usr/bin/perl

use strict;
use warnings;

use Test::More tests => 7;

use_ok('Biblio::Folio::Util', qw(_utc_datetime));

my $millennium_est = '2001-01-01T05:00:00.000+0000';
my $millennium_utc = '2001-01-01T00:00:00.000+0000';

is(_utc_datetime('2001-01-01'),      $millennium_est, 'local date');
is(_utc_datetime('20010101T000000'), $millennium_est, 'local date and time');
is(_utc_datetime(978325200),         $millennium_est, 'local date in seconds');

is(_utc_datetime('2001-01-01TZ'),         $millennium_utc, 'UTC date');
is(_utc_datetime('20010101T000000+0000'), $millennium_utc, 'UTC date and time');
is(_utc_datetime(978307200),              $millennium_utc, 'UTC date in seconds');
