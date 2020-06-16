package Biblio::Folio::Site::LocalDB::SourceRecords;

use strict;
use warnings;

use base qw(Biblio::Folio::Site::LocalDB);

use Biblio::Folio::Util qw(_utc_datetime);
use Encode qw(decode encode);

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
    $sth->execute;
    my ($last) = $sth->fetchrow_array;
    $sth->finish;
    return $last || 0;
}

sub fetch {
    my ($self, $instance_id) = @_;
    my $sth = $self->sth(q{SELECT * FROM source_records WHERE instance_id = ?});
    $sth->execute($instance_id);
    my $row = $sth->fetchrow_hashref;
    die "no such instance: $instance_id\n" if !$row;
    my $marc = $row->{'marc'};
    #_utf8_on($marc);  # XXX
    # my $marc = decode('UTF-8', $row->{'marc'});  # eval { encoded('UTF-8', $row->{'marc'}) };
    # die "instance: MARC data can't be encoded as UTF-8: $instance_id\n" if !defined $marc;
    #substr($marc, 0, 5) = sprintf('%05d', length $marc);
    $row->{'marcref'} = \$marc;
    return $row;
}

sub marcref {
    my $self = shift;
    my $sth;
    my $marc;
    if (@_ == 1) {
        my ($instance_id) = @_;
        $sth = $self->sth(q{SELECT marc FROM source_records WHERE instance_id = ?});
        $sth->execute($instance_id);
        my @row = $sth->fetchrow_array;
        die "no such instance: $instance_id\n" if !@row;
        $marc = $row[0];  # eval { encode('UTF-8', $row[0]) };
        #_utf8_on($marc);  # XXX
        # substr($marc, 0, 5) = sprintf('%05d', length $marc);
        # die "instance: MARC data can't be encoded as UTF-8: $instance_id\n" if !defined $marc;
        return \$marc;
    }
    else {
        die "\$db->marc(\$instance_id)";
    }
}

sub sync {
    my ($self, %arg) = @_;
    my $site = $self->site;
    my $file = $self->file;
    my $exists = -e $file;
    my $progress = $arg{'progress'};
    my $dbh = $self->dbh;
    $site->dont_cache(qw(instance source_record holdings_record));
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
    my $ended = time;
    $dbh->begin_work;
    $self->sth(q{
        UPDATE  syncs
        SET     ended = ?, status = ?, num_records = ?
        WHERE   began = ?
    })->execute($ended, 'ok', $n, $began);
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
