package Biblio::Folio::Site::Stash;

sub new {
    my $cls = shift;
    my $self = bless { @_ }, $cls;
    return $self->init;
}

sub name { $_[0]{'name'} }
sub site { $_[0]{'site'} }
sub dbh { $_[0]{'dbh'} }

sub init {
    my ($self) = @_;
    my $file = $self->{'file'}
        ||= $self->site->file("var/stash/$name.sqlite");
    if (! -e $file) {
        die "no such stash DB file: $file";
    }
    my $dbh = $self->dbh;
    $dbh->do("ATTACH DATABASE ? AS $name", {}, $file);
    $self->{'attached'} = 1;
    return $self;
}

sub put {
    my ($self, $table) = splice @_, 0, 2;
    while (@_) {
        my ($id, $data) = splice @_, 0, 2;
        my @cols = keys %$data;
        my @vals = @$data{@cols};
        my $sql = sprintf(
            q{INSERT OR REPLACE INTO %s.%s (%s) VALUES (%s)},
            $name,
            $table,
            join(',', 'id', @cols),
            $dbh->quote($id),
            join(',', '?', map { '?' } @vals ),
        );
        my $sth = $stash->{'sth'}{$sql} ||=
            $stash->prepare($sql);
        $sth->execute(@vals);
    }
}

sub get {
    my ($self, $table, @keys) = @_;
}

sub DESTROY {
    my ($self) = @_;
}

1;
