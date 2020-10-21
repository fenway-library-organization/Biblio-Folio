package Biblio::Folio::Site::LocalDB::Instances;

use strict;
use warnings;

use base qw(Biblio::Folio::Site::LocalDB);

use Biblio::Folio::Site::LocalDB::Instances::Update;
use Biblio::Folio::Util qw(_utc_datetime _is_uuid _bool2int);
use Biblio::Folio::Site::MARC;
use Scalar::Util qw(blessed);

# Update types
use constant FULL        => 'full';
use constant INCREMENTAL => 'incremental';

# Update statuses
use constant RUNNING   => 'running';
use constant PARTIAL   => 'partial';
use constant COMPLETED => 'completed';
use constant FAILED    => 'failed';

# Diagnostic levels
use constant WARNING => 0;
use constant ERROR   => 1;

sub create {
    my $self = shift;
    if (@_ % 2) {
        my $ref = ref $_[0];
        if ($ref eq 'DBI::db') {
            unshift @_, 'dbh';
        }
        elsif (!$ref) {
            unshift @_, 'file';
        }
    }
    $self = $self->new(@_) if !ref $self;
    my $file = $self->file;
    my $dbh = $self->dbh;
    my $sql;
    if (defined $file) {
        (my $sqlfile = $file) =~ s{\.[^/.]+$}{.sql};
        if (-e $sqlfile) {
            open my $fh, '<', $sqlfile or die "open $sqlfile: $!";
            local $/;
            $sql = <$fh>;
        }
    }
    if (!defined $sql) {
        $sql = <<'EOS';
CREATE TABLE instances (
    id                  VARCHAR UNIQUE PRIMARY KEY,
    hrid                VARCHAR UNIQUE NOT NULL,
    source_type         VARCHAR     NULL,
    source              BLOB        NULL,
    last_modified       REAL    NOT NULL,
    suppressed          INTEGER NOT NULL DEFAULT 0,
    deleted             INTEGER NOT NULL DEFAULT 0,
    update_id           INTEGER,
    /*
    CONSTRAINT CHECK    (suppressed IN (0, 1)),
    CONSTRAINT CHECK    (deleted IN (0, 1)),
    */
    FOREIGN KEY         (update_id) REFERENCES updates(id)
);
CREATE TABLE holdings (
    instance_id         VARCHAR NOT NULL,
    holdings_record_id  VARCHAR NOT NULL,
    suppressed          INTEGER DEFAULT 0,
    deleted             INTEGER DEFAULT 0,
    /*
    CONSTRAINT CHECK    (deleted IN (0, 1)),
    */
    FOREIGN KEY         (instance_id) REFERENCES instances(id)
);
CREATE TABLE updates (
    id                  INTEGER PRIMARY KEY,
    type                VARCHAR NOT NULL DEFAULT 'incremental',
    status              VARCHAR NOT NULL DEFAULT 'starting',
    query               VARCHAR     NULL,
    comment             VARCHAR     NULL,
    began               REAL    NOT NULL,
    ended               REAL        NULL,
    after               REAL        NULL,
    before              REAL        NULL,
    max_last_modified   REAL        NULL,
    num_records         INTEGER NOT NULL DEFAULT 0,
    num_errors          INTEGER NOT NULL DEFAULT 0
    /*
    ,
    CONSTRAINT CHECK    (ended >= began),
    CONSTRAINT CHECK    (type IN ('full', 'incremental', 'one-time')),
    CONSTRAINT CHECK    (status IN ('starting', 'running', 'partial', 'completed', 'failed'))
    */
);
CREATE TABLE diagnostics (
    instance_id         VARCHAR NOT NULL,
    update_id           INTEGER     NULL,
    record_number       INTEGER     NULL,
    level               INTEGER NOT NULL DEFAULT 1,  /* 0 = warning, 1 = error */
    message             VARCHAR NOT NULL,
    /*
    CONSTRAINT CHECK    (level IN (0, 1)),
    */
    FOREIGN KEY         (instance_id) REFERENCES instances(id),
    FOREIGN KEY         (update_id) REFERENCES updates(id)
);
/* Indexes on instances */
CREATE INDEX instances_hrid_index              ON instances (hrid);
CREATE INDEX instances_hrid_length_index       ON instances (length(hrid));
CREATE INDEX instances_source_type_index       ON instances (source_type);
CREATE INDEX instances_last_modified_index     ON instances (last_modified);
CREATE INDEX instances_suppressed_index        ON instances (suppressed);
CREATE INDEX instances_deleted_index           ON instances (deleted);
CREATE INDEX instances_update_id_index         ON instances (update_id);
/* Indexes on instance holdings */
CREATE INDEX holdings_instance_id_index        ON holdings (instance_id);
CREATE INDEX holdings_holdings_record_id_index ON holdings (holdings_record_id);
/* Indexes on updates */
CREATE INDEX updates_type_index                ON updates (type);
CREATE INDEX updates_status_index              ON updates (status);
CREATE INDEX updates_began_index               ON updates (began);
CREATE INDEX updates_max_last_modified_index   ON updates (max_last_modified);
/* Indexes on diagnostics */
CREATE INDEX diagnostics_instance_index        ON diagnostics (instance_id);
CREATE INDEX diagnostics_update_index          ON diagnostics (update_id);
CREATE INDEX diagnostics_type_index            ON diagnostics (level);
EOS
    }
    $dbh->begin_work;
    $dbh->do($_) for grep { !m{^/\* .+ \*/$} } split /;\n/, $sql;
    $dbh->commit;
    return $self;
}

sub max_after {
    my ($self) = @_;
    my $sth = $self->sth(q{
        SELECT  max(after)
        FROM    updates
        WHERE   type IN (?, ?)
        AND     after IS NOT NULL
        AND     status IN (?, ?)
    });
    $sth->execute(FULL, INCREMENTAL, RUNNING, COMPLETED);
    my ($max) = $sth->fetchrow_array;
    $sth->finish;
    return $max || 0;
}

sub max_last_modified {
    my ($self, $update_id) = @_;
    my ($sql, @params);
    if (defined $update_id) {
        $sql = q{
            SELECT  max_last_modified
            FROM    updates
            WHERE   id = ?
        };
        @params = ($update_id);
    }
    else {
        $sql = q{
            SELECT  max(last_modified)
            FROM    instances
        };
    }
    my $sth = $self->sth($sql);
    $sth->execute(@params);
    my ($last) = $sth->fetchrow_array;
    $sth->finish;
    return $last || 0;
}

sub current_update_id {
    my ($self) = @_;
    my $sth = $self->sth(q{
        SELECT  id
        FROM    updates
        WHERE   type IN (?, ?)
        AND     status IN (?, ?)
        ORDER   BY began DESC
    });
    $sth->execute(FULL, INCREMENTAL, RUNNING, PARTIAL);
    my ($last) = $sth->fetchrow_array;
    $sth->finish;
    return $last || 0;
}

sub last_sync {
    my ($self) = @_;
    my $sth = $self->sth(q{
        SELECT  id
        FROM    updates
        WHERE   type = IN (?, ?)
        AND     status = ?
        ORDER   BY began DESC
    });
    $sth->execute(FULL, INCREMENTAL, COMPLETED);
    my ($last) = $sth->fetchrow_array;
    $sth->finish;
    return $self->update($last) if defined $last;
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

sub store {
    my ($self, %arg) = @_;
    my ($records, $progress_handler, $error_handler) = @arg{qw(records progress_handler error_handler)};
    my ($sth, @results);
    my $num_errors = 0;
    foreach my $record (@$records) {
        my ($instance_id, $instance_hrid) = @$record{qw(instance_id instance_hrid)};
        my ($update, $record_number)      = @$record{qw(update record_number)};
        my ($error, $warning)             = @$record{qw(error warning)};
        my ($source_type, $source)        = @$record{qw(source_type source)};
        my ($last_modified, $deleted, $suppressed)
                                          = @$record{qw(last_modified deleted suppressed)};
        my ($marcjson, $marcref, $marc)   = @$record{qw(marcjson marcref marc)};
        my @marc_args;
        my $no_identifier = !defined $instance_id || !defined $instance_hrid;
        eval {
            if (defined $error) {
                return;
            }
            elsif (defined $source_type && $source_type ne 'MARC') {
                return $error = "can't store anything but MARC source records";
            }
            elsif (defined $source) {
                @marc_args = ('marcref' => \$source) if $no_identifier;
            }
            elsif (defined $marcref) {
                $source = $$marcref;
                @marc_args = ('marcref' => $marcref) if $no_identifier;
            }
            elsif (defined $marcjson) {
                @marc_args = ('marcjson' => $marcjson) if $no_identifier;  # , 'must_encode_as' => 'UTF-8');
                $last_modified //= _utc_datetime($marcjson->{'metadata'}{'updatedDate'}, '%s.%J');
                $deleted //= $marcjson->{'deleted'};
                $suppressed //= $marcjson->{'additionalInfo'}{'suppressDiscovery'};
            }
            elsif (!defined $marc) {
                return $error = "can't make a storable source record without MARC input";
            }
            elsif (!ref $marc) {
                $source = $marc;
                $marc = Biblio::Folio::Site::MARC->new('marcref' => \$source) if $no_identifier;  # , 'must_encode_as' => 'UTF-8');
            }
            # Create a Biblio::Folio::Site::MARC instance if necessary
            if (@marc_args) {
                $marc = Biblio::Folio::Site::MARC->new(@marc_args);
            }
            if (!defined $instance_id) {
                my $msg = "MARC record doesn't have a 999 field with an instance ID";
                my $field = $marc->field('999ff')
                    or return $error = $msg;
                $instance_id = $field->subfield('i')
                    // return $error = $msg;
            }
            if (!defined $instance_hrid) {
                my $field = $marc->field('001') or return $error = "instance $instance_id doesn't have an 001 field";
                $instance_hrid = $field->value;
            }
            if (!defined $source) {
                $source = eval { $marc->as_marc21 } or return $error = "instance $instance_id can't be rendered as a MARC record";
            }
        };
        if (defined $error) {
            log_diagnostic(%$record, 'level' => ERROR, 'message' => $error);
            $error_handler->($record_number, [$record]);
            $num_errors++;
        }
        else {
            my $update_id = $update ? $update->id : undef;
            if (defined $warning) {
                log_diagnostic(%$record, 'level' => WARNING, 'message' => $warning);
            }
            $sth ||= $self->sth(q{
                INSERT OR REPLACE INTO instances (id, hrid, source_type, source, last_modified, update_id, deleted, suppressed)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            });
            $sth->execute(
                $instance_id, $instance_hrid, $source_type, $source, $last_modified,
                $update_id, _bool2int($deleted), _bool2int($suppressed)
            );
            $record->{'ok'} = 1;
            next;
        }
    }
    return @_ if wantarray;
    return $num_errors == 0;
}

sub log_diagnostic {
    my ($self, %arg) = @_;
    my $sth = $self->sth(q{
        INSERT INTO diagnostics (instance_id, update_id, record_number, level, message)
        VALUES (?, ?, ?, ?, ?)
    });
    $sth->execute(@arg{qw(instance_id update_id record_number level message)});
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

sub sync {
    my ($self, %arg) = @_;
    my $update = $self->update(%arg);
    $update->run;
}

sub update {
    my ($self, @args) = @_;
    if (@args % 2) {
        die "odd number of property arguments" if @args != 1;
        unshift @args, 'id';
    }
    return Biblio::Folio::Site::LocalDB::Instances::Update->new(
        'db' => $self,
        @args,
    );
}

sub update_fameflower {
    my ($self, %arg) = @_;
    my $file = $self->file;
    my $exists = -e $file;
    if (!$exists) {
        $self->create;
    }
    my $site = $self->site;
    $site->dont_cache(qw(instance source_record holdings_record));
    my ($query, $comment, $progress_handler, $error_handler) = @arg{qw(query comment progress_handler error_handler)};
    $progress_handler ||= sub {};
    $error_handler ||= $progress_handler;
    my @errors;
    my ($type, $record_this_update, $update_id);
    my $dbh = $self->dbh;
    if (defined $query) {
        ### die "sync with a query is not allowed";
        $type = 'one-time';
        $query = sprintf q{(%s) and state==ACTUAL}, $query;
        $record_this_update = 0;
    }
    else {
        $type = INCREMENTAL;
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
    my $searcher = $site->searcher('source_record')->query($query)->limit(1000);
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
                        $marc = Biblio::Folio::Site::MARC->new('marcjson' => $marcjson);
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
                $error_handler->($n, \@errors);
                next;
            }
            if (defined $source) {
                my $last_modified = _utc_datetime($source_record->{'metadata'}{'updatedDate'}, '%s');
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
        $progress_handler->($n, \@errors);
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
        })->execute($ended, COMPLETED, $n, $update_id);
        $dbh->commit;
    }
}

sub clean {
    my ($self, %arg) = @_;
    my ($age, $limit) = @arg{qw(age limit)};
    my $sql = "DELETE FROM updates WHERE num_records = 0 AND status = ?";
    my @params = (COMPLETED);
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
