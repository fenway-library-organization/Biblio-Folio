package Biblio::Folio::Site::LocalSourceDB;

use strict;
use warnings;

use Biblio::Folio::Util qw(_utc_datetime);
use DBI;

sub new {
    my $cls = shift;
    unshift @_, 'file' if @_ % 2;
    my $self = bless { @_ }, $cls;
    $self->init;
    return $self;
}

sub site { @_ > 1 ? $_[0]{'site'} = $_[1] : $_[0]{'site'} }
sub file { @_ > 1 ? $_[0]{'file'} = $_[1] : $_[0]{'file'} }

sub init {
    my ($self) = @_;
    my $file = $self->{'file'};
    my $exists = -e $file;

}

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
    my $sth = $self->dbh->prepare($sql);
    return $sth;
}

sub create {
    my $self = shift;
    $self = $self->new(@_) if !ref $self;
    my $file = @_ ? shift : $self->file;
    die "no file name given when creating a local source DB?"
        if !defined $file;
    my $dbh = $self->dbh;
    $dbh->begin_work;
    $dbh->do($_) for split /;\n/, <<'EOS';
CREATE TABLE source_records (
    instance_id  VARCHAR PRIMARY KEY,
    marc         VARCHAR,
    last_updated VARCHAR NOT NULL,
    sync_id      INT,
    deleted      INT,
    suppress     INT
);
CREATE TABLE syncs (
    began        INT PRIMARY KEY,
    ended        INT NULL,
    status       VARCHAR,
    comment      VARCHAR,
    num_records  INT
);
CREATE INDEX source_record_last_updated_index ON source_records(last_updated);
CREATE INDEX source_record_deleted_index ON source_records(deleted);
CREATE INDEX syncs_began_index ON syncs(began);
EOS
    $dbh->commit;
    return $self;
}

sub last_sync {
    my ($self) = @_;
    my $sth = $self->sth(q{SELECT max(began) FROM syncs WHERE status = 'ok'});
    my ($last) = $sth->fetchrow_array;
    $sth->finish;
    return $last || 0;
}

sub sync {
    my ($self, %arg) = @_;
    my $site = $self->site;
    my $file = $self->file;
    my $exists = -e $file;
    my $progress = $arg{'progress'};
    my $dbh = $self->dbh;
    $site->dont_cache({ map { $_ => 1 } qw(instance source_record holdings_record)});
    my $query = q{recordType==MARC and state=ACTUAL};
    if (!$exists) {
        $self->create;
    }
    else {
        my $last = $self->last_sync;
        if ($last) {
            $query = sprintf '%s and metadata.updatedDate > "%s"', $query, _utc_datetime($last);
        }
    }
    my $searcher = $site->searcher('source_record', '@query' => $query, '@limit' => 1000);
    my $n = 0;
	my $began = time;
	$self->sth('INSERT INTO syncs (began, num_records) VALUES (?, 0)')->execute($began);
	my $sth_ins = $self->sth(<<'EOS');
INSERT OR REPLACE INTO source_records (instance_id, marc, last_updated, sync_id, deleted, suppress)
VALUES (?, ?, ?, ?, ?, ?)
EOS
    while (my @source_records = $searcher->next) {
        $dbh->begin_work;
        foreach my $source_record (@source_records) {
			next if $source_record->{'errorRecord'};
         	my $instance_id = $source_record->{'externalIdsHolder'}{'instanceId'}
                or next;
            my $marc = $source_record->{'rawRecord'}{'content'};
			my $last_updated = $source_record->{'metadata'}{'updatedDate'};
			my $deleted = $source_record->{'deleted'} ? 1 : 0;
			my $suppress = $source_record->{'additionalInfo'}{'suppressDiscovery'} ? 1 : 0;
			$sth_ins->execute($instance_id, $marc, $last_updated, $began, $deleted, $suppress);
            1;
        }
        $dbh->commit;
        $n += @source_records;
        $progress->($n) if $progress;
    }
    $dbh->begin_work;
	$self->sth('UPDATE syncs SET ended = ?, status = ?, num_records = ?')->execute(time, 'ok', $n);
    $dbh->commit;
}

### my $sth = $dbh->prepare(q{INSERT INTO });
### $marc_fetch = sub {
###     my ($instance, $instance_id) = @_;
###     $sth->execute($instance_id);
###     my ($marc) = $sth->fetchrow_array;
###     die "no such instance: $instance_id\n" if !defined $marc;
###     return Biblio::Folio::Site::MARC->new('marcref' => \$marc);
### };

1;
