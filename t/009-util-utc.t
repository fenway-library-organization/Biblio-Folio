#!/usr/bin/perl

use strict;
use warnings;

use Test::More;  # tests => 7;

my $millennium_est = '2001-01-01T05:00:00.000+0000';
my $millennium_utc = '2001-01-01T00:00:00.000+0000';

my @tests = (
    [ 'local date',             '2001-01-01'           => $millennium_est ],
    [ 'local date and time',    '20010101T000000'      => $millennium_est ],
    [ 'local date in seconds',  978325200              => $millennium_est ],
    [ 'UTC date',               '2001-01-01TZ'         => $millennium_utc ],
    [ 'UTC date and time',      '20010101T000000+0000' => $millennium_utc ],
    [ 'UTC date in seconds',    978307200              => $millennium_utc ],
    [ 'epoch to UTC',           1586620020, '%Y-%m-%dT%H:%M:%SZ', '2020-04-11T15:47:00Z' ],
    [ 'UTC to epoch',           '2020-04-11T15:47:00Z', '%s' => 1586620020 ],
    [ 'tz (-0400) to epoch',    '2020-04-11T11:47:00-0400', '%s' => 1586620020 ],
    [ 'tz (+1200) to epoch',    '2020-04-12T03:47:00+1200', '%s' => 1586620020 ],
    [ 'epoch to epoch',         1586620020, '%s' => 1586620020 ],
# Fractions of a second
    [ 'epoch to UTC with ms',   '1586620020.123', '%Y-%m-%dT%H:%M:%S.%JZ', '2020-04-11T15:47:00.123Z' ],
    [ 'epoch to UTC with us',   '1586620020.123456', '%Y-%m-%dT%H:%M:%S.%KZ', '2020-04-11T15:47:00.123456Z' ],
    [ 'UTC with ms to epoch',   '2020-04-11T15:47:00.123Z', '%s.%J' => '1586620020.123' ],
    [ 'UTC with us to epoch',   '2020-04-11T15:47:00.123456Z', '%s.%K' => '1586620020.123456' ],
    [ 'epoch to epoch with ms', '1586620020.001', '%s.%J' => '1586620020.001' ],
    [ 'epoch to epoch with us', '1586620020.000001', '%s.%K' => '1586620020.000001' ],
);

plan tests => (1 + @tests);

use_ok('Biblio::Folio::Util', qw(_utc_datetime));
foreach (@tests) {
    my $descrip = shift @$_;
    my $expected = pop @$_;
    my $answer = eval { _utc_datetime(@$_) };
    is($answer, $expected, $descrip);
}

__END__
is(_utc_datetime('2001-01-01'),      $millennium_est, 'local date');
is(_utc_datetime('20010101T000000'), $millennium_est, 'local date and time');
is(_utc_datetime(978325200),         $millennium_est, 'local date in seconds');

is(_utc_datetime('2001-01-01TZ'),         $millennium_utc, 'UTC date');
is(_utc_datetime('20010101T000000+0000'), $millennium_utc, 'UTC date and time');
is(_utc_datetime(978307200),              $millennium_utc, 'UTC date in seconds');

