package Biblio::Folio::Site::LocalDB;

use strict;
use warnings;

use Scalar::Util qw(weaken);

# ---------------- Folderol to set up DB state constants, etc. -----------------
our @EXPORT_OK;
my %is_state;
sub exportable($) {
    my ($symbol, $value) = @_;
    push @EXPORT_OK, $symbol;
    $is_state{$symbol} = 1;
    $value = $symbol if !defined $value;
    return ($symbol, $value);
}
# States
use constant exportable 'DB_READ_WRITE';
use constant exportable 'DB_READ_ONLY';
use constant exportable 'DB_SYNC';
use constant exportable 'DB_BEGIN_SYNC';
use constant exportable 'DB_END_SYNC';
require Exporter;
our @ISA = qw(Exporter);
# ------------------------------------------------------------------------------

use Biblio::Folio::Util qw(_unindent);

# --- Constructors

sub new {
    my $cls = shift;
    unshift @_, 'file' if @_ % 2;
    my $self = bless { @_ }, $cls;
    $self->init;
    return $self;
}

sub create {
    my $proto = shift;
    if (!ref $proto) {
        # $db = $cls->create($file, ...);
        return $proto->new(
            @_,
            'create' => 1,
        );
    }
    else {
        my $self = $proto;
        my @tables = $self->tables;
        my @indexes = $self->indexes;
        die "no tables?" if !@tables;
        my $dbh = $self->dbh;
        $dbh->begin_work;
        my $ok;
        eval {
            while (@tables > 1) {
                my ($t, $table) = splice @tables, 0, 2;
                $self->_create_table($t, $table);
            }
            while (@indexes > 1) {
                my ($i, $index) = splice @indexes, 0, 2;
                $self->_create_index($i, $index);
            }
            $ok = 1;
        };
        if ($ok) {
            $dbh->commit;
            return $self;
        }
        else {
            $dbh->rollback;
            die "can't create database: " . $dbh->errstr;
        }
    }
}

# --- Class methods

sub tables { }  # Must be overridden if create is not overridden
sub indexes { }

# --- Accessors

sub site { @_ > 1 ? $_[0]{'site'} = $_[1] : $_[0]{'site'} }
sub name { @_ > 1 ? $_[0]{'name'} = $_[1] : $_[0]{'name'} }
sub file { @_ > 1 ? $_[0]{'file'} = $_[1] : $_[0]{'file'} }

sub state {
    my $self = shift;
    return $self->{'state'} if !@_;
    my ($state) = @_;
    die "invalid DB state: $state" if !$is_state{$state};
    return $self->{'state'} = $state;
}

# --- Public methods

sub dbh {
    my $self = shift;
    return $self->{'dbh'} = shift if @_;
    my $file = $self->{'file'};
    return $self->{'dbh'} ||= DBI->connect("dbi:SQLite:dbname=$file", '', '', {
        'AutoCommit' => 1,
        'RaiseError' => 1,
    });
}

sub sth {
    my ($self, $sql) = @_;
    my $sth = $self->{'sth'}{$sql} ||= $self->dbh->prepare($sql);
    return $sth;
}

# --- Private methods

sub init {
    my ($self) = @_;
    my $site = $self->{'site'};
    weaken($self->{'site'});
    my $name = $self->{'name'};
    my $file = $self->{'file'}
        ||= $site->file("var/db/$name.db");
    $self->{'state'} ||= DB_READ_WRITE;
    my $exists = -e $file;
    my $create = delete $self->{'create'} || $self->can('create');
    $self->{'sth'} = {};
    my $dbh;
    if (!$exists) {
        if (!$create) {
            die "no such local DB file: $file";
        }
        elsif (!ref $create) {
            $create = $self->can('create')
                or die 'unimplemented method: ' . ref($self) . '::create';
        }
        elsif (ref($create) eq 'ARRAY') {
            my @sql = @$create;
            $create = sub {
                my (undef, $dbh) = @_;
                $dbh->begin_work;
                $dbh->do($_) for @sql;
                $dbh->commit;
            };
        }
        $dbh = $self->dbh;
        $create->($self, $dbh);
    }
    # $self->{'tables'} = {};
    # $self->_discover_schema;
    $self->{'transaction_level'} = 0;
    return $self;
}

### sub _discover_schema {
###     my ($self) = @_;
###     my $name = $self->name;
###     my $dbh = $self->dbh;
###     my $sth = $dbh->table_info(undef, $name, '%', 'table');
###     my $tables = $self->tables;
###     while (my @row = $sth->fetchrow_array) {
###         my $table = $row[2];
###         my @keys = $dbh->primary_key(undef, $name, $table);
###         next if @keys != 1;
###         $tables->{$table} = { 'id_column' => $keys[0] };
###     }
### }

sub _create_table_or_index {
    my ($self, $what_type, $what_name, $sql) = @_;
    $sql =~ s{;\s*\z}{}ms;
    if ($sql !~ m{\A\s*CREATE\s+$what_type\s+}i) {
        if ($sql =~ s{^\A\s*\(}{}ms) {
            die "unbalanced parens: \($sql" if $sql !~ s{\)\s*\z}{}ms;
        }
        $sql = "(\n" . _unindent($sql) . "\n)" if $what_type eq 'TABLE';
        $sql = sprintf 'CREATE %s %s (%s)', uc $what_type, $what_name, $sql;
    }
    else {
        $sql =~ s{
            \A
            \s*
            CREATE \s+ $what_type \s+ (\S+)
        }{
            die if $1 !~ /^(%s|$what_name)$/i;
            sprintf('CREATE %s %s', uc $what_type, $what_name);
        }xmsie or die "unparseable SQL: $sql"
    }
    my $dbh = $self->dbh;
    $dbh->do($sql);
    return $self;
}

sub _create_table {
    # my ($self, $table, $sql) = @_;
    splice @_, 1, 0, 'TABLE';
    goto &_create_table_or_index;
}

sub _create_index {
    # my ($self, $index, $sql) = @_;
    splice @_, 1, 0, 'INDEX';
    goto &_create_table_or_index;
}

sub transact {
    my ($self, $sub) = @_;
    my $dbh = $self->dbh;
    $dbh->begin_work if $self->{'transaction_level'}++ == 0;
    my $ok;
    eval {
        $sub->();
        $dbh->commit if --$self->{'transaction_level'} == 0;
        $ok = 1;
    };
    if (!$ok) {
        $dbh->rollback;
        die $@;
    }
}

sub DESTROY {
    my ($self) = @_;
    my $dbh = $self->{'dbh'};
    $dbh->disconnect if $dbh;
}

1;
