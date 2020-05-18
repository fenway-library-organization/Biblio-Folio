package Biblio::Folio::Site::Stash;

use strict;
use warnings;

sub new {
    my $cls = shift;
    unshift @_, 'name' if @_ % 2;  # Biblio::Folio::Site::Stash->new($name);
    my $self = bless { @_ }, $cls;
    return $self->init;
}

sub create {
# Biblio::Folio::Site::Stash->create(
#     'name' => $name,
#     'id_column' => 'instance_id',
#     'tables' => {
#         'foo' => 'CREATE TABLE foo (instance_id INT PRIMARY KEY, ...)',
#         'bar' => '(id INTEGER PRIMARY KEY, ...)',
#         'baz' => 'id INTEGER PRIMARY KEY, ...',
#     },
#     'indexes' => {
#     },
# );
    my $cls = shift;
    return $cls->new(
        @_,
        'create' => 1,
    );
}

sub main { $_[0]{'main'} }
sub name { $_[0]{'name'} }
sub site { $_[0]{'site'} }
sub dbh { $_[0]{'dbh'} }

# Settable (why??)
sub id_column { @_ > 1 ? $_[0]{'id_column'} = $_[1] : $_[0]{'id_column'} }
sub sth_cache { @_ > 1 ? $_[0]{'sth_cache'} = $_[1] : $_[0]{'sth_cache'} }

sub init {
    my ($self) = @_;
    my $site = $self->site;
    my $name = $self->{'name'} || 'main';
    my $dbh = $self->{'dbh'} or die "stash without a DB handle";
    my $file = $self->{'file'}
        ||= $site->file("var/db/$name.db");
    my $exists = -e $file;
    my $create = delete $self->{'create'};
    if ($name eq 'main') {
        $dbh->do("ATTACH DATABASE ? AS $name", {}, $file);
    }
    if (!$exists) {
        die "no such stash DB file: $file" if !$create;
        my ($tables, $indexes);
        if ($name eq 'main') {
            $tables = {
                'stashes' => q{
                    CREATE TABLE %s (
                        name VARCHAR PRIMARY KEY,
                        created_epoch INTEGER DEFAULT strftime('%s','now')
                    );
                 },
            };
            $indexes = {
                'stashes_by_name' => q{CREATE INDEX %s ON stashes (name)},
                'stashes_by_created_epoch' => q{CREATE INDEX %s ON stashes (created_epoch)},
            };
        }
        else {
            ($tables, $indexes) = @$self{qw(tables indexes)};
        }
        $dbh->begin_work;
        while (my ($t, $table) = each %{ $tables || {} }) {
            $self->create_table($t, $table);
        }
        while (my ($i, $index) = each %{ $indexes || {} }) {
            $self->create_index($i, $index);
        }
        $dbh->commit;
    }
    # Don't cache if $self->{'sth_cache'} is undef
    $self->{'sth_cache'} = {}
        if !exists $self->{'sth_cache'};
    $self->{'id_column'} ||= 'id';
    $self->{'attached'} = 1;
    $self->{'tables'} = {};
    $self->_discover_schema;
    return $self;
}

sub _discover_schema {
    my ($self) = @_;
    my $name = $self->name;
    my $dbh = $self->dbh;
    my $sth = $dbh->table_info(undef, $name, '%', 'table');
    my $tables = $self->tables;
    while (my @row = $sth->fetchrow_array) {
        my $table = $row[2];
        my @keys = $dbh->primary_key(undef, $name, $table);
        next if @keys != 1;
        $tables->{$table} = { 'id_column' => $keys[0] };
    }
}
    
sub _create_table_or_index {
    my ($self, $what_type, $what_name, $sql) = @_;
    my $stash_name = $self->name;
    $sql =~ s{;\s*\z}{}ms;
    if ($sql !~ m{\A\s*CREATE\s+$what_type\s+}i) {
        if ($sql =~ s{^\A\s*\(}{}ms) {
            die "unbalanced parens: \($sql" if $sql !~ s{\)\s*\z}{}ms;
        }
        $sql = "(\n" . unindent($sql) . "\n)" if $what_type eq 'TABLE';
        $sql = sprintf 'CREATE %s %s.%s (%s)', uc $what_type, $stash_name, $what_name, $sql;
    }
    else {
        $sql =~ s{
            \A
            \s*
            CREATE \s+ $what_type \s+ (\S+)
        }{
            die if $1 !~ /^(%s|$what_name)$/i;
            sprintf('CREATE %s %s.%s', uc $what_type, $stash_name, $what_name);
        }xmsie or die "unparseable SQL: $sql"
    }
    my $dbh = $self->dbh;
    $dbh->do($sql);
    return $self;
}

sub unindent {
    my @lines = map { split /(?<=\n)/ } @_;
    my ($first_indent_len, $last_indent_len) = map { m{^(\s+)} ? length($1) : 0 } ($lines[0], $lines[-1]);
    my $imax = $first_indent_len > $last_indent_len ? $first_indent_len : $last_indent_len;
    if ($imax == 0) {
        # First and last lines are not indented
        return @_ if wantarray;
        return join("\n", @lines);
    }
    my $rx = "[ ]{1,$imax}";
    $rx = qr/$rx/;
    s/^$rx// for @lines;
    return @lines if wantarray;
    my $end = $_[0] =~ /\n\z/ ? "\n" : '';
    return join("\n", @lines) . $end;
}

sub create_table {
    # my ($self, $table, $sql) = @_;
    splice @_, 1, 0, 'TABLE';
    goto &_create_table_or_index;
}

sub create_index {
    # my ($self, $index, $sql) = @_;
    splice @_, 1, 0, 'INDEX';
    goto &_create_table_or_index;
}

sub put {
    # $stash->put($table, $id1 => {...}, $id2 => {...}, ...);
    my ($self, $table) = splice @_, 0, 2;
    die 'usage: $stash->put($table, $id1 => \%data1, $id2 => \%data2, ...)'
        if @_ % 2;
    my $name = $self->name;
    my $dbh = $self->dbh;
    my $id_column = $self->id_column;
    while (@_) {
        my ($id, $data) = splice @_, 0, 2;
        my @cols = sort keys %$data;
        my @vals = @$data{@cols};
        my $sql = sprintf(
            q{INSERT OR REPLACE INTO %s.%s (%s) VALUES (%s)},
            $name,
            $table,
            join(',', $id_column, @cols),
            $dbh->quote($id),
            join(',', '?', map { '?' } @vals ),
        );
        # INSERT OR REPLACE INTO harvest.source_records (deleted, instance_id, last_updated, marc, suppress) VALUES (?, ?, ?, ?, ?)
        my $sth = $self->sth($sql);
        $sth->execute(@vals);
    }
}

sub sth {
    my ($self, $sql) = @_;
    my $dbh = $self->dbh;
    my $cache = $self->sth_cache;
    return $dbh->prepare($sql) if !$cache;
    return $cache->{$sql} ||= $dbh->prepare($sql);
}

sub get {
    my ($self, $table, @keys) = @_;
}

sub DESTROY {
    my ($self) = @_;
}

1;
