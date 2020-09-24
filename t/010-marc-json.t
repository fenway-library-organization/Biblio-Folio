use strict;
use warnings;

use Test::More;

my %name2marcjson = map {
    m{^t/files/(.+)\.marcjson$};
    $1 => $_;
} glob("t/files/*.marcjson");

my %name2marc = map {
    m{^t/files/(.+)\.mrc$} ? ($1 => $_) : ();
} glob("t/files/*.mrc");

my %both = map { $_ => 1 } keys %name2marcjson;
foreach (keys %both) {
    delete $both{$_} if !defined $name2marc{$_};
}

plan tests => 2 + 2 * scalar(keys %both);

use_ok('Biblio::Folio::Util', qw(_json_decode));
use_ok('Biblio::Folio::Site::MARC');
foreach my $name (keys %both) {
    my $j = read_file($name2marcjson{$name});
    my $m = read_file($name2marc{$name});
    my $marcjson = Biblio::Folio::Site::MARC->new('marcjson' => _json_decode($j));
    my $marc = Biblio::Folio::Site::MARC->new('marcref' => \$m);
    is($marcjson->as_marc21, $marc->as_marc21);
    is_deeply($marcjson->as_marcjson, $marc->as_marcjson);
}

sub read_file {
    my ($f) = @_;
    open my $fh, '<', $f or die "open $f: $!";
    local $/;
    my $str = <$fh>;
    close $fh or die "close $f: $!";
    return $str;
}
