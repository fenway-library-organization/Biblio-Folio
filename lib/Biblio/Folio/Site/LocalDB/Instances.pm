package Biblio::Folio::Site::LocalDB::Instances;

use strict;
use warnings;

use base qw(Biblio::Folio::Site::LocalDB);

use Biblio::Folio::Util qw(_utc_datetime);
#use Encode qw(decode encode);

sub create {
    my $self = shift;
    $self = $self->new(@_) if !ref $self;
    my $file = @_ ? shift : $self->file;
    die "no file name given when creating a local source DB?"
        if !defined $file;
    my $dbh = $self->dbh;
    $dbh->begin_work;
    $dbh->do($_) for grep { !m{^/\* .+ \*/$} } split /;\n/, <<'EOS';
CREATE TABLE instances (
    id                  VARCHAR UNIQUE PRIMARY KEY,
    hrid                VARCHAR UNIQUE NOT NULL,
    source_type         VARCHAR NULL,
    source              VARCHAR NULL,
    last_modified       VARCHAR NOT NULL,
    suppressed          INTEGER,
    deleted             INTEGER,
    update_id           INTEGER,
    /*
    CONSTRAINT CHECK    (suppressed IN (0, 1)),
    CONSTRAINT CHECK    (deleted IN (0, 1)),
    */
    FOREIGN KEY         (update_id) REFERENCES updates(began)
);
CREATE TABLE holdings (
    instance_id         VARCHAR NOT NULL,
    holdings_record_id  VARCHAR NOT NULL,
    deleted             INTEGER DEFAULT 0,
    /*
    CONSTRAINT CHECK    (deleted IN (0, 1)),
    */
    FOREIGN KEY         (instance_id) REFERENCES instances(id)
);
CREATE TABLE updates (
    id                  INTEGER PRIMARY KEY,
    began               INTEGER NOT NULL,
    ended               INTEGER NULL,
    type                VARCHAR NOT NULL DEFAULT 'sync',
    query               VARCHAR,
    comment             VARCHAR,
    status              VARCHAR NOT NULL DEFAULT 'running',
    num_records         INTEGER NOT NULL DEFAULT 0
    /*
    ,
    CONSTRAINT CHECK    (ended >= began),
    CONSTRAINT CHECK    (type IN ('sync', 'update')),
    CONSTRAINT CHECK    (status IN ('ok', 'failed'))
    */
);
/* Indexes on instances */
CREATE INDEX instances_hrid_index              ON instances (hrid);
CREATE INDEX instances_source_type_index       ON instances (source_type);
CREATE INDEX instances_last_modified_index     ON instances (last_modified);
CREATE INDEX instances_suppressed_index        ON instances (suppressed);
CREATE INDEX instances_deleted_index           ON instances (deleted);
/* Indexes on instance holdings */
CREATE INDEX holdings_instance_id_index        ON holdings (instance_id);
CREATE INDEX holdings_holdings_record_id_index ON holdings (holdings_record_id);
/* Indexes on updates */
CREATE INDEX updates_began_index               ON updates (began);
CREATE INDEX updates_type_index                ON updates (type);
CREATE INDEX updates_status_index              ON updates (status);
EOS
    $dbh->commit;
    return $self;
}

sub last_sync {
    my ($self) = @_;
    my $sth = $self->sth(q{
        SELECT  max(began)
        FROM    updates
        WHERE   type = 'sync'
        AND     status = 'ok'});
    $sth->execute;
    my ($last) = $sth->fetchrow_array;
    $sth->finish;
    return $last || 0;
}

sub last_update {
    my ($self) = @_;
    my $sth = $self->sth(q{
        SELECT  max(began)
        FROM    updates
        WHERE   type = 'update'
        AND     status = 'ok'});
    $sth->execute;
    my ($last) = $sth->fetchrow_array;
    $sth->finish;
    return $last || 0;
}

sub fetch {
    my ($self, $instance_id) = @_;
    my $sth = $self->sth(q{SELECT * FROM instances WHERE id = ?});
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
        $sth = $self->sth(q{SELECT marc FROM instances WHERE id = ?});
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
    my ($query, $comment, $progress) = @arg{qw(query comment progress)};
    my ($type, $record_this_update, $update_id);
    my $dbh = $self->dbh;
    if (defined $query) {
        $type = 'update';
        $query = sprintf q{(%s) and state==ACTUAL}, $query;
        $record_this_update = 0;
    }
    else {
        $type = 'sync';
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
        $self->sth(q{
            INSERT INTO updates (began, type, query, comment, num_records) VALUES (?, ?, ?, ?, 0)}
        )->execute($began, $type, $query, $comment);
        $update_id = $dbh->sqlite_last_insert_rowid;
        $dbh->commit;
    }
    # Get new and updated source records
    my $searcher = $site->searcher('source_record', '@query' => $query, '@limit' => 1000);
    my $n = 0;
    my $sth_ins;
    while (my @source_records = $searcher->next) {
        $dbh->begin_work;
        foreach my $source_record (@source_records) {
            my $instance_id = $source_record->{'externalIdsHolder'}{'instanceId'}
                or next;
            next if $source_record->{'errorRecord'};
            my $source_type = $source_record->{'recordType'};
            my $source = $source_record->{'rawRecord'}{'content'};
            my $last_modified = $source_record->{'metadata'}{'updatedDate'};
            my $deleted = $source_record->{'deleted'} ? 1 : 0;
            my $suppressed = $source_record->{'additionalInfo'}{'suppressDiscovery'} ? 1 : 0;
            $sth_ins ||= $self->sth(q{
                INSERT OR REPLACE INTO instances (id, source_type, source, last_modified, update_id, deleted, suppressed)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            });
            $sth_ins->execute($instance_id, $source_type, $source, $last_modified, $update_id, $deleted, $suppressed);
            1;
        }
        $dbh->commit;
        $n += @source_records;
        $progress->($n) if $progress;
    }
    # Add hrids for new instances
    my $sth_nulls = $self->sth(q{
        SELECT  id
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
                WHERE   id = ?
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
            WHERE   id = ?
        })->execute($ended, 'ok', $n, $update_id);
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
