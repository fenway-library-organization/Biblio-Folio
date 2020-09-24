package Biblio::Folio::Site::LocalDB::Instances;

use strict;
use warnings;

use base qw(Biblio::Folio::Site::LocalDB);

use Biblio::Folio::Util qw(_utc_datetime);
use Biblio::Folio::Site::MARC;
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
CREATE INDEX instances_hrid_length_index       ON instances (length(hrid));
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
        $sth = $self->sth(q{SELECT source, source_type FROM instances WHERE id = ? AND source_type = 'MARC'});
        $sth->execute($instance_id);
        my ($marc, $type) = $sth->fetchrow_array;
        die "no such instance: $instance_id\n" if !defined $marc;
        die "not a MARC record: $instance_id\n" if $type ne 'MARC';
        return \$marc;
    }
    else {
        die "\$db->marc(\$instance_id)";
    }
}

sub sync { goto &update_goldenrod }

sub update_goldenrod {
    my ($self, %arg) = @_;
    my $file = $self->file;
    my $exists = -e $file;
    my $full;
    if (!$exists) {
        $self->create;
        $full = 1;
    }
    my $site = $self->site;
    $site->dont_cache(qw(instance source_record holdings_record));
    my ($query, $limit, $comment, $progress, $error) = @arg{qw(query limit comment progress error)};
    $progress ||= sub {};
    $error ||= $progress;
    my @errors;
    my ($type, $update_id);
    my $record_this_update = !$arg{'dry_run'};
    my $dbh = $self->dbh;
    if (defined $query) {
        ### die "sync with a query is not allowed";
        $type = 'update';
        $record_this_update = 0;
    }
    else {
        $type = 'sync';
        my $last = $self->last_sync;
        if ($last) {
            $query = sprintf 'metadata.updatedDate > "%s"', _utc_datetime($last);
        }
        else {
            $full = 1;
        }
        $record_this_update //= 1;
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
    if ($full) {
        # Get all source records
        $limit ||= 1000;
        my $searcher = $site->searcher('source_record', '@limit' => $limit);
        my $n = 0;
        while (my @source_records = $searcher->next) {
            $dbh->begin_work;
            foreach my $source_record (@source_records) {
                $n++;
                my ($source, $source_type, $err, $instance_hrid);
            }
            $dbh->commit;
            $progress->($n, \@errors);
        }
    }
    else {
        # Get new and updated instances, then the source record for each
        $limit ||= 100;
        my $searcher = $site->searcher('instance', '@query' => $query, '@limit' => $limit);
        my $n = 0;
        my $sth_ins;
        while (my @instances = $searcher->next) {
            $dbh->begin_work;
            foreach my $instance (@instances) {
                $n++;
                my ($ok, $err, $iid, $ihrid, $source_record, $source_type, $marc21);
                eval {
                    $iid = $instance->id;
                    $ihrid = $instance->hrid;
                    $source_record = $instance->source_record;  # API call (#@$!?)
                    $source_type = $source_record->recordType;
                    die "instance $iid source record is not in MARC format"
                        if $source_type ne 'MARC';
                    my $marcjson = $source_record->{'parsedRecord'}{'content'};
                    my $state = $source_record->{'state'};
                    if ($state eq 'ACTUAL') {
                        my $marc = Biblio::Folio::Site::MARC->new('marcjson' => $marcjson);
                        if (!defined $marc) {
                            $err = "instance $iid source record can't be parsed";
                        }
                        else {
                            $marc21 = eval { $marc->as_marc21 };
                            if (!defined $marc21) {
                                $err = "instance $ihrid can't be exported as MARC21";
                            }
                        }
                    }
                    $ok = 1;
                };
                if (!$ok) {
                    ($err) = split /\n/, $@ if !defined $err;
                    $err = 'unknown error' if !length $err;
                }
                if (defined $err) {
                    push @errors, $err;
                    $error->($n, \@errors);
                }
                elsif (defined $marc21) {
                    my $last_modified = $source_record->{'metadata'}{'updatedDate'};
                    my $deleted = $source_record->{'deleted'} ? 1 : 0;
                    my $suppressed = $source_record->{'additionalInfo'}{'suppressDiscovery'} ? 1 : 0;
                    $sth_ins ||= $self->sth(q{
                        INSERT OR REPLACE INTO instances (id, hrid, source_type, source, last_modified, update_id, deleted, suppressed)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    });
                    $sth_ins->execute($iid, $ihrid, $source_type, $marc21, $last_modified, $update_id, $deleted, $suppressed);
                }
            }
            $dbh->commit;
            $progress->($n, \@errors);
        }
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

sub update {
    my ($self, %arg) = @_;
    my $file = $self->file;
    my $exists = -e $file;
    if (!$exists) {
        $self->create;
    }
    my $site = $self->site;
    $site->dont_cache(qw(instance source_record holdings_record));
    my ($query, $comment, $progress, $error) = @arg{qw(query comment progress error)};
    $progress ||= sub {};
    $error ||= $progress;
    my @errors;
    my ($type, $record_this_update, $update_id);
    my $dbh = $self->dbh;
    if (defined $query) {
        ### die "sync with a query is not allowed";
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
            $n++;
            my ($source, $source_type, $err, $instance_hrid);
            my $instance_id = $source_record->{'externalIdsHolder'}{'instanceId'};
            if (!defined $instance_id) {
                $err = "record #$n doesn't have an instance ID";
            }
            else {
                $source_type = $source_record->{'recordType'};
                my $marcjson = $source_record->{'parsedRecord'}{'content'};
                # my $source = $source_record->{'rawRecord'}{'content'};
                $instance_hrid = $instance_id;  # XXX Hack
                if ($source_type eq 'MARC') {
                    my ($marc, $hrid);
                    eval {
                        $marc = Biblio::Folio::Site::MARC->new('marcjson' => \$marcjson);
                        $hrid = $marc->field('001')->value;
                        $instance_hrid = $hrid;
                        $source = eval { $marc->as_marc21 };
                    };
                    if (!defined $marc) {
                        $err = "instance $instance_id source record can't be parsed";
                    }
                    elsif (!defined $hrid) {
                        $err = "instance $instance_id doesn't have an hrid";
                    }
                    elsif (!defined $source) {
                        $err = "instance $instance_hrid can't be exported as MARC21";
                    }
                }
                else {
                    $err = "record $instance_id does not have a MARC source record";
                }
            }
            if (defined $err) {
                push @errors, $err;
                $error->($n, \@errors);
                next;
            }
            if (defined $source) {
                my $last_modified = $source_record->{'metadata'}{'updatedDate'};
                my $deleted = $source_record->{'deleted'} ? 1 : 0;
                my $suppressed = $source_record->{'additionalInfo'}{'suppressDiscovery'} ? 1 : 0;
                $sth_ins ||= $self->sth(q{
                    INSERT OR REPLACE INTO instances (id, hrid, source_type, source, last_modified, update_id, deleted, suppressed)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                });
                $sth_ins->execute($instance_id, $instance_hrid, $source_type, $source, $last_modified, $update_id, $deleted, $suppressed);
            }
        }
        $dbh->commit;
        $progress->($n, \@errors);
    }
    # Add hrids for new instances (hrid set to instance ID, which is 36 bytes long)
    my $sth_nulls = $self->sth(q{
        SELECT  id
        FROM    instances
        WHERE   length(hrid) = 36
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
