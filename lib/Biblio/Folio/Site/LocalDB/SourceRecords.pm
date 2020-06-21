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
CREATE TABLE instances (
    instance_id   VARCHAR PRIMARY KEY,
    instance_hrid VARCHAR NOT NULL,
    marc          VARCHAR /* NULL */,
    last_updated  VARCHAR NOT NULL,
    sync_id       INT,
    deleted       INT,
    suppress      INT
);
CREATE TABLE instance_holdings (
    instance_id         VARCHAR NOT NULL,
    holdings_record_id  VARCHAR NOT NULL,
    deleted             INT DEFAULT 0
);
CREATE TABLE updates (
    began        INT PRIMARY KEY,
    ended        INT NULL,
    status       VARCHAR,
    comment      VARCHAR,
    num_records  INT
);
CREATE INDEX source_record_last_updated_index ON instances(last_updated);
CREATE INDEX source_record_deleted_index ON instances(deleted);
CREATE INDEX syncs_began_index ON syncs(began);
CREATE INDEX instance_holdings_index_instance_id ON instance_holdings (instance_id);
CREATE INDEX instance_holdings_index_holdings_record_id ON instance_holdings (holdings_record_id);
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
    my $sth = $self->sth(q{SELECT * FROM instances WHERE instance_id = ?});
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
        $sth = $self->sth(q{SELECT marc FROM instances WHERE instance_id = ?});
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
    die "sync with a query is not allowed"
        if defined $arg{'query'};
    goto &update;
}

sub update {
    my ($self, %arg) = @_;
    my $file = $self->file;
    my $exists = -e $file;
    if (!$exists) {
        $self->create;
    }
    my $site = $self->site;
    $site->dont_cache(qw(instance source_record holdings_record));
    my $query = $arg{'query'};
    my $progress = $arg{'progress'};
    my $dbh = $self->dbh;
    my $record_this_update;
    if (defined $query) {
        $query = sprintf q{(%s) and state==ACTUAL}, $query;
        $record_this_update = 0;
    }
    else {
        my $last = $self->last_sync;
        if ($last) {
            $query = sprintf 'state==ACTUAL and metadata.updatedDate > "%s"', _utc_datetime($last);
        }
        else {
            $query = qq{state==ACTUAL};
        }
        $record_this_update = 1;
    }
    my $began = time;
    if ($record_this_update) {
        $dbh->begin_work;
        $self->sth('INSERT INTO updates (began, num_records, query) VALUES (?, 0, ?)')->execute($began, $query);
        $dbh->commit;
    }
    # Get new and updated source records
    my $searcher = $site->searcher('source_record', '@query' => $query, '@limit' => 1000);
    my $n = 0;
    my $sth_ins = $self->sth(<<'EOS');
INSERT OR REPLACE INTO instances (instance_id, marc, last_updated, sync_id, deleted, suppress)
VALUES (?, ?, ?, ?, ?, ?)
EOS
    while (my @source_records = $searcher->next) {
        $dbh->begin_work;
        foreach my $source_record (@source_records) {
            my $instance_id = $source_record->{'externalIdsHolder'}{'instanceId'}
                or next;
            my $marc;
            if ($source_record->{'recordType'} =~ /^MARC/) {
                next if $source_record->{'errorRecord'};
                $marc = $source_record->{'rawRecord'}{'content'};
            }
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
    # Add hrids for new instances
    my $sth_nulls = $self->sth(q{
        SELECT  instance_id
        FROM    instances
        WHERE   hrid IS NULL
    });
    my $sth_upd;
    $sth_nulls->execute;
    my $rows = $sth_nulls->fetchall_arrayref;
    my @ids = map { @$_ } @$rows;
    my $isearcher = $site->searcher('instance')->by('set' => \@ids)->limit(1000);
    while (my @instances = $isearcher->next) {
        $dbh->begin_work;
        foreach my $instance (@instances) {
            my $id = $instance->id;
            my $hrid = $instance->hrid;
            $sth_upd ||= $self->sth(q{
                UPDATE  instances
                SET     hrid = ?
                WHERE   instance_id = ?
            });
            $sth_upd->execute($hrid, $id);
        }
        $dbh->commit;
    }
    my $ended = time;
    if ($record_this_update) {
        $dbh->begin_work;
        $self->sth(q{
            UPDATE  updates
            SET     ended = ?, status = ?, num_records = ?
            WHERE   began = ?
        })->execute($ended, 'ok', $n, $began);
        $dbh->commit;
    }
}

sub clean {
    my ($self, %arg) = @_;
    my ($age, $limit) = @arg{qw(age limit)};
    my $sql = "DELETE FROM updates WHERE num_records = 0 AND status = 'ok'";
    my @params;
    if (defined $age) {
        $sql .= " AND ended <= ?";
        push @params, time - $age;
    }
    if (defined $limit) {
        $limit =~ /^[0-9]+$/ or die "invalid clean limit: $limit";
        $sql .= " LIMIT $limit";
    }
    $self->sth($sql)->execute(@params);
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
