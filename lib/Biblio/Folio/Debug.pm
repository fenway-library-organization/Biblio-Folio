package Biblio::Folio::Debug;

use strict;
use warnings;

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT = qw(
    hd
);

sub hd($);

sub hdbytes {
    my $str = shift;
    my $i = 0;
    printf STDERR "Length (bytes): %d\n", bytes::length $str;
    while ($str =~ m{(.{1,16})}g) {
        my $chunk = $1;
        my @hexbytes = map { sprintf('%02x', $_) } unpack('C*', $chunk);
        $chunk =~ s/[^[:print:]]/./g;
        printf STDERR "  %04x | %-47s | %s\n", $i, join(' ', @hexbytes), $chunk;
        $i += 16;
    }
}

sub hdchars {
    my $str = shift;
    my $i = 0;
    printf STDERR "Length (chars): %d\n", length $str;
    while ($str =~ m{(.{1,16})}g) {
        my $chunk = $1;
        my @hexbytes = map { sprintf('%04x', $_) } unpack('W*', $chunk);
        $chunk =~ s/[^[:print:]]/./g;
        printf STDERR "  %04x | %-79s | %s\n", $i, join(' ', @hexbytes), $chunk;
        $i += 16;
    }
}

sub hd($) {
    my $str = shift;
    my $is_utf8 = utf8::is_utf8($str);
    print STDERR "-" x 80, "\n";
    print STDERR "UTF-8 flag: ", $is_utf8 ? "set\n" : "NOT set\n";
    if ($str !~ /[^\x00-\x7f]/) {
        print STDERR "All ASCII: Yes\n";
        hdbytes($str);
    }
    else {
        print STDERR "All ASCII: No\n";
        my ($charstr, $bytestr) = ($str, $str);
        if ($is_utf8) {
            utf8::encode($bytestr);
        }
        else {
            utf8::decode($charstr);
        }
        hdchars($charstr);
        hdbytes($bytestr);
    }
}
