#!/usr/bin/perl

use strict;
use warnings;

use Test::More tests => 6;

use_ok('Biblio::Folio::Util', qw(_json_encode _json_decode _json_begin _json_append _json_end));

    my %baz = ( 'baz' => 'qux' );
    my @bar = ( 'bar', \%baz );
    my %foo = ( 'foo' => \@bar );
    my @plus = ( 1, 2, 3, { 'four' => 4 }, [ 5..6 ], undef, 8 );
    my %foo_plus = ( 'foo' => [@bar, @plus] );

is_deeply(
    _json_decode(_json_encode(\%foo)),
    \%foo,
    '_json_decode(_json_encode($hash))'
);

{
        my $out;
        open my $fh, '>', \$out or die "Couldn't redirect output to a scalar";
        my $ctx = _json_begin(+{%foo}, 'foo', $fh);

    ok(defined $ctx, '_json_begin($hash, $key, $fh)');

        foreach (@plus) {
            _json_append($ctx, $_);
        }
        _json_end($ctx);

    is_deeply(
        _json_decode($out),
        \%foo_plus,
        '_json_append() and _json_end() on a hash'
    );
}

{
        my $out;
        open my $fh, '>', \$out or die "Couldn't redirect output to a scalar";
        my $ctx = _json_begin(\@bar, undef, $fh);

    ok(defined $ctx, '_json_begin($array, undef, $fh)');

        foreach (@plus) {
            _json_append($ctx, $_);
        }
        _json_end($ctx);

    is_deeply(
        _json_decode($out),
        [@bar, @plus],
        '_json_append() and _json_end() on an array'
    );
}
