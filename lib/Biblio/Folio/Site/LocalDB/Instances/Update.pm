package Biblio::Folio::Site::LocalDB::Instances::Update;

use strict;
use warnings;

use Biblio::Folio::Util qw(_utc_datetime);
use Biblio::Folio::Site::LocalDB::Instances::Constants qw(:status);
use Biblio::Folio::Site::MARC;
use Time::HiRes qw(time);

use constant SRS_DATETIME_FORMAT => '%Y-%m-%dT%H:%M:%S.%JZ';

sub new {
    my $cls = shift;
    my $self = bless { @_ }, $cls;
    $self->init;
    return $self;
}

sub db { @_ > 1 ? $_[0]{'db'} = $_[1] : $_[0]{'db'} }
sub dry_run { @_ > 1 ? $_[0]{'dry_run'} = $_[1] : $_[0]{'dry_run'} }
sub batch_size { @_ > 1 ? $_[0]{'batch_size'} = $_[1] : $_[0]{'batch_size'} }
sub max_update_size { @_ > 1 ? $_[0]{'max_update_size'} = $_[1] : $_[0]{'max_update_size'} }
sub searcher { @_ > 1 ? $_[0]{'searcher'} = $_[1] : $_[0]{'searcher'} }
sub offset { @_ > 1 ? $_[0]{'offset'} = $_[1] : $_[0]{'offset'} }

sub id { @_ > 1 ? $_[0]{'id'} = $_[1] : $_[0]{'id'} }
sub began { @_ > 1 ? $_[0]{'began'} = $_[1] : $_[0]{'began'} }
sub ended { @_ > 1 ? $_[0]{'ended'} = $_[1] : $_[0]{'ended'} }
sub type { @_ > 1 ? $_[0]{'type'} = $_[1] : $_[0]{'type'} }
sub status { @_ > 1 ? $_[0]{'status'} = $_[1] : $_[0]{'status'} }
sub comment { @_ > 1 ? $_[0]{'comment'} = $_[1] : $_[0]{'comment'} }
sub query { @_ > 1 ? $_[0]{'query'} = $_[1] : $_[0]{'query'} }
sub after { @_ > 1 ? $_[0]{'after'} = $_[1] : $_[0]{'after'} }
sub before { @_ > 1 ? $_[0]{'before'} = $_[1] : $_[0]{'before'} }
sub num_records { @_ > 1 ? $_[0]{'num_records'} = $_[1] : $_[0]{'num_records'} }
sub num_errors { @_ > 1 ? $_[0]{'num_errors'} = $_[1] : $_[0]{'num_errors'} }
sub max_last_modified { @_ > 1 ? $_[0]{'max_last_modified'} = $_[1] : $_[0]{'max_last_modified'} }

sub DESTROY { }

sub init {
    my ($self) = @_;
    my $db = $self->db;
    my $site = $db->site;
    my $file = $db->file;
    $site->dont_cache(qw(instance source_record holdings_record));
    my $update_id = $self->id;
    if (!-e $file) {
        die "DB file is empty, so update ID $update_id is meaningless: $file"
            if defined $update_id;
        $db->create;
    }
    elsif (defined $update_id) {
        $self->load;
    }
    $self->{'batch_size'} ||= 1_000;
    $self->{'max_update_size'} ||= 10_000;
    $self->{'errors'} ||= [];
    return $self;
}

sub begin {
    my ($self, $status) = @_;
    my $update_id = $self->id;
    if (defined $update_id) {
        # Continue an existing update
        # $self->load;
        $status ||= 'continuing';
        my $prev_status = $self->status;
        if ($prev_status eq 'failed') {
            die "update has already failed: $update_id";
        }
        elsif ($prev_status eq 'completed') {
            die "update has already been completed: $update_id";
        }
        elsif ($prev_status ne 'partial') {
            die "update cannot be continued when status is $status: $update_id";
        }
    }
    else {
        $status ||= 'starting';
        if (!defined $self->began) {
            $self->began(sprintf('%.3f', time));
        }
    }
    $self->make_searcher;
    $self->status($status);
    $self->save;
}

sub make_searcher {
    my ($self) = @_;
    return if defined $self->searcher;
    my $site = $self->db->site;
    my $offset = $self->offset || $self->num_records;
    my $batch_size = $self->batch_size;
    my ($after, $before, $max_last_modified) = ($self->after, $self->before, $self->max_last_modified);
    if (defined $max_last_modified) {
        $after = $max_last_modified;  # XXX +/- 0.001 ???
        $offset = 0;
    }
    elsif (!defined $before && !defined $after) {
        my $last = $self->db->last_sync;
        if ($last) {
            $after = $last->max_last_modified;  # XXX +/- 0.001 ???
            $offset = 0;
        }
    }
    my $searcher = $site->searcher('source_record')->uri('/source-storage/source-records');
    $searcher->param('orderBy' => 'updatedDate,ASC')->offset($offset)->limit($batch_size);
    if (defined $before) {
        $searcher->param('updatedBefore' => _utc_datetime($before, SRS_DATETIME_FORMAT));
    }
    if (defined $after) {
        $searcher->param('updatedAfter' => _utc_datetime($after, SRS_DATETIME_FORMAT));
    }
    else {
        # Full
    }
    return $self->searcher($searcher);
}

sub load {
    my ($self) = @_;
    my $db = $self->db;
    my $dbh = $db->dbh;
    my $sth = $db->sth(q{
        SELECT  *
        FROM    updates
        WHERE   id = ?
    });
    $sth->execute($self->id);
    my $row = $sth->fetchrow_hashref;
    $sth->finish;
    die "no such update: $self->id" if !$row;
    %$self = (
        %$self,
        %$row,
    );
    return $self;
}

sub save {
    my ($self) = @_;
    return if $self->dry_run;
    my $type = $self->type;
    my $update_id = $self->id;
    my $db = $self->db;
    my $dbh = $db->dbh;
    my @columns = qw(began ended type status comment query after before num_records num_errors max_last_modified);
    my $sth;
    my @params = (@$self{@columns});
    if (defined $update_id) {
###     my $old_sql = q{
###         UPDATE  updates
###         SET     began = ?,
###                 ended = ?,
###                 type = ?,
###                 status = ?,
###                 comment = ?,
###                 query = ?,
###                 after = ?,
###                 before = ?,
###                 num_records = ?,
###                 num_errors = ?,
###                 max_last_modified = ?
###         WHERE   id = ?
###     };
        my $joiner = q{,
                    };
        my $sql = sprintf q{
            UPDATE  updates
            SET     %s
            WHERE   id = ?
        }, join($joiner, map { "$_ = ?" } @columns);
        $sth = $db->sth($sql);
        push @params, $update_id;
    }
    else {
###     my $old_sql = q{
###         INSERT  INTO updates
###                 (began, ended, type, status, comment, query, after, before, num_records, num_errors, max_last_modified)
###         VALUES  (?,     ?,     ?,    ?,      ?,       ?,     ?,     ?,      ?,           ?,          ?                )
###     };
        my $sql = sprintf q{
            INSERT  INTO updates
                    (%s)
            VALUES  (%s)
        }, join(', ', @columns), join(', ', map { sprintf "%*s", length($_), '?' } @columns);
        $sth = $db->sth($sql);
    }
    $dbh->begin_work;
    $sth->execute(@params);
    $self->{'id'} ||= $dbh->sqlite_last_insert_rowid;
    $dbh->commit;
    return $self;
}

sub sms {
    my $t = @_ ? shift : time;
    $t =~ s/(?<=\.[0-9]{3})[0-9]+//;  # Strip digits after the milliseconds
    return _utc_datetime($t, '%s.%J')
}

sub run {
    my ($self) = @_;
    my $update_id = $self->id;
    my $searcher = $self->searcher;
    my $db = $self->db;
    my $dbh = $db->dbh;
    my $progress = $self->{'progress'} || sub {};
    my $error = $self->{'error'} || $progress;
    my $sth_ins;
    my @errors;
    my $batch_size = $self->{'batch_size'};
    my $max_last_modified = $self->max_last_modified || sms(0);
    my $n = $self->num_records;
    my $maxn = $n + $self->max_update_size;
    BATCH:
    while ($n < $maxn) {
        if (my @source_records = $searcher->next) {
            $dbh->begin_work;
            RECORD:
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
                    $self->num_errors($self->num_errors + 1);
                    $error->($n, \@errors);
                    next RECORD;
                }
                if (defined $source) {
                    my $last_modified = sms($source_record->{'metadata'}{'updatedDate'});
                    $max_last_modified = $last_modified if $last_modified > $max_last_modified;
                    my $deleted = $source_record->{'deleted'} ? 1 : 0;
                    my $suppressed = $source_record->{'additionalInfo'}{'suppressDiscovery'} ? 1 : 0;
                    $sth_ins ||= $db->sth(q{
                        INSERT OR REPLACE INTO instances (id, hrid, source_type, source, last_modified, update_id, deleted, suppressed)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    });
                    $sth_ins->execute($instance_id, $instance_hrid, $source_type, $source, $last_modified, $update_id, $deleted, $suppressed);
                }
            }
            $self->max_last_modified($max_last_modified) if $max_last_modified;  # 0.000 is false
            $self->num_records($n);
            $progress->($n, \@errors);
            $dbh->commit;
            $self->save;
        }
        else {
            # Premature end of data
            last BATCH;
        }
    }
    1;
}

sub errstr {
    return "error: $@";  # TODO
}

sub end {
    my ($self, $status) = @_;
    $self->status($status ||= 'completed');
    $self->ended(sms())
        if $status eq 'completed';
    $self->save;
}

1;

