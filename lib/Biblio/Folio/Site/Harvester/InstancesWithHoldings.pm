package Biblio::Folio::Site::Harvester::InstancesWithHoldings;

use strict;
use warnings;

use Biblio::Folio::Site::Harvester;
use Biblio::Folio::Util qw(_utc_datetime);
use Biblio::LDP;
use Time::HiRes;
use POSIX qw(strftime);
use Encode qw(encode);

use vars qw(@ISA);
@ISA = qw(Biblio::Folio::Site::Harvester);

# Harvesting statuses
use constant NONE  => 'none';
use constant BEGUN => 'begun';
use constant ENDED => 'ended';

use constant HAS_MARC => 1;
use constant HAS_IDENTIFIERS => 2;
use constant HAS_INSTANCE => 3;
use constant HAS_HOLDINGS => 4;

sub init {
    my ($self) = @_;
    my $site = $self->{'site'};
    my $folio = $site->folio;
    $self->{'batch_size'} ||= 1000;
    $self->{'small_batch_size'} ||= 25;
    # $self->{'ldp'} ||= Biblio::LDP->new('root' => $site->root, 'name' => $site->name);
    $self->{'status'} = NONE;
}

sub batch_size { @_ > 1 ? $_[0]{'batch_size'} = $_[1] : $_[0]{'batch_size'} }
sub small_batch_size { @_ > 1 ? $_[0]{'small_batch_size'} = $_[1] : $_[0]{'small_batch_size'} }

sub ldp {
    my $self = shift;
    return $self->{'ldp'} = shift if @_;
    my $site = $self->site;
    return $self->{'ldp'} ||= Biblio::LDP->new('root' => $site->root, 'name' => $site->name);
}

sub local_instances_db {
    my $self = shift;
    return $self->{'local_instances_db'} = shift if @_;
    my $site = $self->site;
    return $self->{'local_instances_db'} ||= $site->local_instances_database;
}

sub query { @_ > 1 ? $_[0]{'query'} = $_[1] : $_[0]{'query'} }
sub state { @_ > 1 ? $_[0]{'state'} = $_[1] : $_[0]{'state'} }
sub source_dbh { @_ > 1 ? $_[0]{'source_dbh'} = $_[1] : $_[0]{'source_dbh'} }

sub begin {
    my ($self, %arg) = @_;
    die "harvesting not ended"
        if $self->{'status'} eq BEGUN;
    $self->{'state'} = {
        'began' => time,
        'seen' => {},
        'records' => {},
        'queues' => { empty_queues() },
        %arg,
    };
    $self->{'status'} = BEGUN;
    return $self;
}

sub empty_queues {
    return (
        'instance' => [],
        'holdings_record' => [],
        'holdings_record_by_instance_id' => [],
        'item' => [],
    );
}

sub end {
    my ($self) = @_;
    $self->flush;
    $self->{'status'} = ENDED;
}

sub see_source_record {
    my ($self, $source_record, %sr) = @_;
    my $err = $source_record->{'errorRecord'};
    if ($err) {
        my $parsed = $err->{'content'};
        my $descrip = $err->{'description'};
        print STDERR "source record error: $descrip\n";
        return;
    }
    my $state = $self->state;
    my ($seen, $records, $queues) = @$state{qw(seen records queues)};
    my ($sid, $iid, $marcref, $upd, $del, $sup) = @sr{qw(id instance_id marcref updated deleted suppressed)};
    $sid ||= $source_record->{'id'};
    $iid ||= $source_record->{'externalIdsHolder'}{'instanceId'};
    $upd ||= $source_record->{'metadata'}{'updatedDate'};
    $del ||= $source_record->{'deleted'};
    $sup ||= $source_record->{'additionalInfo'}{'suppressDiscovery'};
    next if $seen->{$sid}++;  # XXX impossible, but must increment
# DO NOT DO THIS!!! next if $seen->{$iid}++;  # XXX impossible, but must increment
    my $hqueue = $queues->{'holdings_record_by_instance_id'};
    my $iqueue = $queues->{'instance'};
    push @$iqueue, $iid;
    push @$hqueue, $iid;
    if (!defined $marcref) {
        my $raw = $source_record->{'rawRecord'}{'content'};
        my $marc = encode('UTF-8', $raw);
        $marcref = \$marc;
    }
    $records->{$iid} = {
        #'source_record' => $source_record,
        'source_record_id' => $sid,
        'instance' => undef,
        'holdings_records' => undef,
        'instance_id' => $iid,
        'marcref' => $marcref,
        'last_updated' => $upd,
        'deleted' => $del,
        'suppressed' => $sup,
        'has' => HAS_MARC|HAS_IDENTIFIERS,
    };
}

sub gather {
    my ($self, %arg) = @_;
    my $since = $arg{'since'};
    if (defined $since) {
        my $datetime = _utc_datetime($since);
        my $cql = qq{metadata.updateDate > "$datetime"};
        my $batch_size = $arg{'batch_size'} || $self->batch_size;
        my $n = 0;
        while (1) {
            if ($arg{'local_instances_db'}) {
                my $lidb = $self->local_instances_db;
                my $num_synced = 0;
                printf STDERR "\r%8d source record(s) synced", $num_synced;
                $lidb->sync('progress' => sub {
                    ($num_synced) = @_;
                    printf STDERR "\r%8d source record(s) synced", $num_synced
                        if $num_synced % 100 == 0;
                });
                printf STDERR "\r%8d source record(s) synced\n", $num_synced;
            }
            $n += $self->gather_instances('@query' => $cql, '@limit' => $batch_size);
            $n += $self->gather_source_records;
            $n += $self->gather_holdings_records('@query' => $cql, '@limit' => $batch_size);
            last if !$n;
            $self->flush;
        }
        return $n;
    }
}

sub gather_source_records {
    my ($self, %search) = @_;
    my $site = $self->site;
    my $state = $self->state;
    my ($seen, $records, $queues) = @$state{qw(seen records queues)};
    my $hqueue = $queues->{'holdings_record_by_instance_id'};
    my $iqueue = $queues->{'instance'};
    my $n = 0;
    if ($search{'@query'}) {
        my $searcher = $self->site->searcher('source_record', %search);
        while (1) {
            my @source_records = $searcher->next;
            $n = @source_records or last;
            foreach my $source_record (@source_records) {
                $self->see_source_record($source_record);
            }
        }
        return $n;
    }
    elsif (@$iqueue) {
        my $size = $self->small_batch_size;
        $size = @$iqueue if @$iqueue < $size;
        my @batch = splice @$iqueue, 0, $size;
        my $lidb = local_instances_db();
        foreach my $iid (@batch) {
            my $sr = $lidb->fetch($iid);
            if ($sr) {
                $records->{$iid} = {
                    #'source_record' => $source_record,
                    'source_record_id' => undef,  # XXX We don't store that in the local source record DB :-(
                    #'instance' => undef,
                    #'holdings_records' => undef,
                    'instance_id' => $iid,
                    'marcref' => $sr->{'marcref'},
                    'last_updated' => $sr->{'last_updated'},
                    'deleted' => $sr->{'deleted'},
                    'suppressed' => $sr->{'suppress'},
                    'has' => HAS_MARC|HAS_IDENTIFIERS,
                };
            }
            else {
                1;
            }
        }
    }
}

sub gather_instances {
    my ($self, %search) = @_;
    my $searcher = $self->site->searcher('instance', %search);
    my $state = $self->state;
    my ($seen, $records, $queues) = @$state{qw(seen records queues)};
    my $hqueue = $queues->{'holdings_record_by_instance_id'};
    my $n = 0;
    while (1) {
        my @instances = $searcher->next;
        $n = @instances or last;
        foreach my $instance (@instances) {
            my $iid = $instance->{'id'};
            my $hrid = $instance->{'hrid'};
            my $record = $instance->{$iid};
            $record->{'instance_hrid'} = $instance->{'hrid'};
            foreach my $k (qw(discoverySuppress deleted)) {
                $record->{$k} = $instance->{$k}
                    if !$record->{$k};
            }
        }
    }
    return $n;
}

sub gather_holdings_records {
    my ($self, %search) = @_;
    my $searcher = $self->site->searcher('holdings_record', %search);
    my $state = $self->state;
    my ($seen, $records, $queues) = @$state{qw(seen records queues)};
    my $n = 0;
    while (1) {
        my @holdings_records = $searcher->next;
        my $n = @holdings_records or last;
        foreach my $holdings_record (@holdings_records) {
            my $hid = $holdings_record->{'id'};
            next if $seen->{$hid}++;  # XXX impossible, but must increment
            my $iid = $holdings_record->{'instanceId'};
            push @{ $records->{$iid}{'holdings_records'} ||= [] }, $holdings_record;
        }
    }
    return $n;
}

sub flush {
    # Output everything we can
    my ($self) = @_;
    my $site = $self->site;
    my $state = $self->state;
    my $seen = $state->{'seen'};
    my $records = $state->{'records'};
    my $queues = $state->{'queues'};
    # Gather all needed holdings records
    my $hqueue = $queues->{'holdings_record_by_instance_id'};
    if (@$hqueue) {
        $self->gather_holdings_records('@set' => $hqueue, '@id_field' => 'instanceId');
        @$hqueue = ();
    }
    # Gather all needed source records and instance data
    my $iqueue = $queues->{'instance'};
    if (@$iqueue) {
        my $searcher = $site->searcher('instance', '@set' => $iqueue, '@limit' => $self->small_batch_size);
        while (my @instances = $searcher->next) {
            # Get all needed source_records, one ... at ... a ... time ...
            foreach my $instance (@instances) {
                my $iid = $instance->{'id'};
                next if $seen->{$iid}++;
                my $record = $records->{$iid};
                next if $record->{'has'} & HAS_MARC;
                my $source_record = $site->source_record('instance' => $iid);
                $self->see_source_record($source_record);
            }
        }
        @$iqueue = ();
    }
    foreach my $id (keys %$records) {
        my $record = $records->{$id};
        my $instance = $record->{'instance'} or next;
        my $marc = Biblio::Folio::Site::MARC->new('marcref' => $record->{'marcref'});
        $marc->garnish(
            'instance' => $instance,
            'deleted' => $record->{'deleted'},
            'suppressed' => $record->{'suppressed'},
        );
        $marc->garnish(map { $_ => $record->{$_} } qw(instance_id instance_hrid source_record_id deleted suppressed));
    }
    foreach my $record (values %$records) {
    }
    %$records = ();
}

sub queue {
    my $self = shift;
    my $what = shift;
    return if !defined $what;
    my $queues = $self->{'state'}{'queues'};
    my $r = ref $what;
    if ($r eq '') {
        my $kind = $what;
        my $queue = $queues->{$kind};
        return $queue if !@_;
        push @$queue, @_;
    }
    elsif ($what->isa('Biblio::Folio::Site::Searcher')) {
        my $kind = $what->kind;
        my $queue = $queues->{$kind};
        push @$queue, $what;
    }
}

### sub _db_create {
###     my ($self) = @_;
###     my @sql = split /;\n/, <<'EOS';
### CREATE TABLE source_records (
###     instance_id  VARCHAR PRIMARY KEY,
###     marc         VARCHAR,
###     last_updated VARCHAR NOT NULL,
###     deleted      INT,
###     suppress     INT
### );
### CREATE INDEX source_record_last_updated ON source_records(last_updated);
### CREATE INDEX source_record_deleted ON source_records(deleted);
### CREATE TABLE checkpoints (
###     id             INT PRIMARY KEY,
###     began          INT NOT NULL,     -- epoch
###     finished       INT,              -- epoch
###     began_folio    VARCHAR NOT NULL, -- e.g., "2020-05-06T16:39:38.724+0000"
###     finished_folio VARCHAR NOT NULL,
###     num_added      INT DEFAULT 0,
###     num_updated    INT DEFAULT 0,
###     num_deleted    INT DEFAULT 0
### );
### CREATE INDEX checkpoints_time_epoch ON checkpoints(time_epoch);
### EOS
###     my $dbh = $self->source_dbh;
###     foreach my $sql (@sql) {
###         $sql =~ s/;\s*\z//;
###         $dbh->do($sql);
###     }
### }
### 
### sub _db_fill {
###     my ($self) = @_;
###     my $dbh = $self->source_dbh;
###     my $timestamp = $self->_db_last_checkpoint;
###     my $site = $self->site;
###     my $searcher = $site->searcher('source_record', 'updatedDate > %s' => $timestamp, '@limit' => 1000);
###     while (my @source_records = $searcher->next(1000)) {
###         foreach (@source_records) {
###             my $iid = $_->{'instanceId'};
###             my $marc = $_->{'rawRecord'};
###             my $t = $_->{'metadata'}{'updatedDate'};
###             my $del = $_->{'deleted'} ? 1 : 0;
###             my $sup = $_->{'discoverySuppress'} ? 1 : 0;
###             1;
###         }
###     }
### }
### 
### sub _db_last_checkpoint {
###     my ($self) = @_;
###     my $dbh = $self->source_dbh;
###     my $sql;
###     if (wantarray) {
###         # my ($timestamp, $epoch) = $self->_db_last_checkpoint;
###         $sql = 'SELECT max(began_folio), max(began) FROM checkpoints WHERE finished IS NOT NULL';
###     }
###     else {
###         $sql = 'SELECT max(began_folio) FROM checkpoints WHERE finished IS NOT NULL';
###     }
###     my $sth = $dbh->prepare($sql);
###     $sth->execute;
###     my @row = $sth->fetchrow_array;
###     @row = (_utc_datetime(0), 0) if !@row;
###     return @row if wantarray;
###     return shift @row;
### }

sub harvest_all {
    my ($self, %arg) = @_;
    my $site = $self->site;
    $self->begin(%arg);
    my $ssearcher = $site->searcher('source_record', '@query' => 'recordType=="MARC"', '@limit' => 1000);
    while (my @source_records = $ssearcher->next(1000)) {
        my (%instance, %holdings_record);
        foreach my $source_record (@source_records) {
            # delete $source_record->{'parsedRecord'};
            my $err = $source_record->{'errorRecord'};
            if ($err) {
                my $parsed = $err->{'content'};
                my $descrip = $err->{'description'};
                print STDERR "source record error: $descrip\n";
                next;
            }
            my $bid = $source_record->{'externalIdsHolder'}{'instanceId'};
            my $raw = $source_record->{'rawRecord'}{'content'};
            my $upd = $source_record->{'metadata'}{'updatedDate'};
            my $del = $source_record->{'deleted'};
            my $sup = $source_record->{'additionalInfo'}{'suppressDiscovery'};
            my $marc = encode('UTF-8', $raw);
            $instance{$bid} = {
                'id' => $bid,
                'marcref' => \$marc,
                'last_updated' => $upd,
                'deleted' => $del,
                'suppressed' => $sup,
            };
        }
        my $bsearcher = $site->searcher('instance', '@set' => [keys %instance], '@batch_size' => 25, '@limit' => 1000);
        my @bids;
        foreach my $instance ($bsearcher->all) {
            my $bid = $instance->{'id'};
            push @bids, $bid;
            $instance{$bid}{'hrid'} = $instance->{'hrid'};
        }
        my $hsearcher = $site->searcher('holdings_record', '@id_field' => 'instanceId', '@set' => \@bids, '@limit' => 1000);
        my @hids;
        foreach my $holdings_record ($hsearcher->all) {
            my $hid = $holdings_record->{'id'};
            my $bid = $holdings_record->{'instanceId'};
            $holdings_record{$hid} = $holdings_record;
            push @{ $instance{$bid}{'holdings'} ||= [] }, $holdings_record;
            push @hids, $hid;
        }
        my $isearcher = $site->searcher('item', '@id_field' => 'holdingsRecordId', '@set' => \@hids, '@limit' => 1000);
        foreach my $item ($isearcher->all) {
            my $iid = $item->{'itemId'};
            my $hid = $item->{'holdingsRecordId'};
            my $holdings_record = $holdings_record{$hid};
            push @{ $instance{$iid}{'holdings'} ||= [] }, $holdings_record;
            push @hids, $holdings_record->{'id'};
        }
    }
    my $batch_size = $arg{'batch_size'} ||= 25;
    my $bsearcher = $site->searcher('instance', '@limit' => $batch_size);
    1;  # TODO
    $self->end;
###     my $dbfile = $self->site->file($arg{'source_db'});
###     my $exists = -e $dbfile;
###     if ($dbfile !~ m{\.[^/.\s]+$} && !$exists) {
###         $dbfile .= '.sqlite';
###         $exists = -e $dbfile;
###     }
###     die "source record DB file doesn't exist: $dbfile"
###         if !$exists && !$arg{'create_db'};
###     die "can't use DBI" if !eval 'use DBI; 1';
###     my $dbh = DBI->connect("dbi:SQLite:dbname=$dbfile", '', '', {
###         'AutoCommit' => 1,
###         'RaiseError' => 1,
###     });
###     $self->source_dbh($dbh);
###     $self->_db_create if !$exists;
###     $self->_db_fill if $arg{'fill_db'};
###     my $sth = $dbh->prepare(q{SELECT marc FROM source_records WHERE instance_id = ?});
###     my $marc_fetch = sub {
###         my ($instance, $instance_id) = @_;
###         $sth->execute($instance_id);
###         my ($marc) = $sth->fetchrow_array;
###         die "no such instance: $instance_id\n" if !defined $marc;
###         return Biblio::Folio::Site::MARC->new('marcref' => \$marc);
###     };
### }
### else {
### my $ldp = $self->ldp;
### my $dbh = $ldp->dbh;
### my %source_table = map { $_->{'name'} eq 'source_records' ? ($_->{'schema'} => $_) : () } $ldp->tables;
### if ($source_table{'public'}) {
###     # TODO
### }
### else {
###     # TODO
### }
### }
}

sub harvest_from_source_records {
    my ($self, $searcher, %arg) = @_;
    my $seen = $arg{'seen'};
    while (my @source_records = $searcher->next(1000)) {
        my %record;
        my (@iqueue, @hqueue);
        foreach my $source_record (@source_records) {
            my $sid = $source_record->{'id'};
            my $iid = $source_record->{'externalIdsHolder'}{'instanceId'};
            next if $seen->{$sid}++;  # XXX impossible, but must increment
            my $err = $source_record->{'errorRecord'};
            if ($err) {
                my $parsed = $err->{'content'};
                my $descrip = $err->{'description'};
                print STDERR "source record error: $descrip\n";
                next;
            }
            my $raw = $source_record->{'rawRecord'}{'content'};
            my $upd = $source_record->{'metadata'}{'updatedDate'};
            my $del = $source_record->{'deleted'};
            my $sup = $source_record->{'additionalInfo'}{'suppressDiscovery'};
            my $marc = encode('UTF-8', $raw);
            $record{$iid} = {
                'instanceId' => $iid,
                'marcref' => \$marc,
                'last_updated' => $upd,
                'deleted' => $del,
                'suppressed' => $sup,
            };
            push @iqueue, $iid;
        }
        # Gather data from all instances for these 1000 source records
        my $isearcher = $self->site->searcher('instance', '@set' => \@iqueue, '@limit' => 25);
        while (my @instances = $isearcher->next(25)) {
            foreach my $instance (@instances) {
                my $iid = $instance->{'id'};
                my $hrid = $instance->{'hrid'};
                my $record = $instance->{$iid};
                foreach my $k (qw(hrid discoverySuppress)) {
                    $record->{$k} = $instance->{$k};
                }
            }
        }
        # Gather data from all instances for these 1000 source records
        my $hsearcher = $self->site->searcher('holdings_record', '@set' => \@iqueue, '@id_field' => 'instanceId', '@limit' => 25);
        foreach my $holdings_record ($hsearcher->all) {
            my $hid = $holdings_record->{'id'};
            next if $seen->{$hid}++;  # XXX impossible, but must increment
            my $iid = $holdings_record->{'instanceId'};
            push @{ $record{$iid}{'holdings_records'} ||= [] }, $holdings_record;
        }
    }
}

# $holdings_remainder_searcher = $site->searcher('holdings_record', '@set' => [keys %b], '@id_field' => 'instanceId');
sub harvest {
    my ($self, %arg) = @_;
    my $site = $self->site;
    my ($all, $query, $since, $batch_size) = @arg{qw(all query since batch_size)};
    my $big_batch_size = $self->batch_size;
    my $bsearcher;
    if ($all) {
        return $self->harvest_all(%arg);
    }
###     elsif ($query) {
###         $bsearcher = $site->searcher('instance', '@query' => $arg{'query'}, '@limit' => $big_batch_size);
###         $self->gather($bsearcher);
###         my @bids = keys %{ $self->seen('instance') };
###         my $hsearcher = $site->searcher('holdings_record', 'id' => [@bids]);
###     }
    elsif (defined $since) {
        return $self->harvest_since(%arg);
    }
    else {
        die "harvesting by query isn't supported yet";
    }
}

sub harvest_since {
    my ($self, %arg) = @_;
    my $since = $arg{'since'};
    $self->begin(%arg);
    $self->gather('since' => $since);
    $self->end;
}

sub harvest_from_instances {
    my ($self, %arg) = @_;
    my $searcher = $arg{'searcher'};
}

1;
