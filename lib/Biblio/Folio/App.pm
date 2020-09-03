package Biblio::Folio::App;

# A FOLIO application

use strict;
use warnings;

use Biblio::Folio;
use Biblio::Folio::Site::MARC;
use Biblio::Folio::Site::MARC::InstanceMaker;
use Biblio::Folio::Classes;
use Biblio::Folio::Util qw(
    _make_hooks _optional _cql_term _use_class _uuid _unbless _utc_datetime _indentf
    FORMAT_MARC FORMAT_JSON FORMAT_TEXT
);

use LWP::UserAgent;
use HTTP::Headers;
use JSON;
use YAML::XS;
use File::Spec;
use Time::HiRes qw(time);
use MARC::Loop qw(marcparse marcfield marcbuild TAG VALREF DELETE IND1 IND2 SUBS);
use File::Basename qw(dirname basename);
use Getopt::Long
    qw(GetOptionsFromArray :config posix_default gnu_compat require_order bundling no_ignore_case);

sub dd;
sub usage;
sub fatal;

sub new {
    my $cls = shift;
    my $self = bless { @_ }, $cls;
    $self->init;
    my $progfile = $self->{'program_file'} ||= $0;
    $self->{'program'} ||= basename($progfile);
    return $self;
}

sub root { @_ > 1 ? $_[0]{'root'} = $_[1] : $_[0]{'root'} }
sub site_name { @_ > 1 ? $_[0]{'site_name'} = $_[1] : $_[0]{'site_name'} }

sub folio { @_ > 1 ? $_[0]{'folio'} = $_[1] : $_[0]{'folio'} }
sub site { @_ > 1 ? $_[0]{'site'} = $_[1] : $_[0]{'site'} }
sub json { @_ > 1 ? $_[0]{'json'} = $_[1] : $_[0]{'json'} }

sub argv { @_ > 1 ? $_[0]{'argv'} = $_[1] : $_[0]{'argv'} }
sub program { @_ > 1 ? $_[0]{'program'} = $_[1] : $_[0]{'program'} }
sub program_file { @_ > 1 ? $_[0]{'program_file'} = $_[1] : $_[0]{'program_file'} }
sub command { @_ > 1 ? $_[0]{'command'} = $_[1] : $_[0]{'command'} }
sub dryrun { @_ > 1 ? $_[0]{'dryrun'} = $_[1] : $_[0]{'dryrun'} }
sub verbose { @_ > 1 ? $_[0]{'verbose'} = $_[1] : $_[0]{'verbose'} }

sub init {
    my ($self) = @_;
    # ???
    return $self;
}

sub run {
    my ($self) = @_;
    my $argv = $self->argv;
    my ($root, $folio, $site_name);
    $self->usage if !@$argv;
    if ($argv->[0] =~ s/^\@(.+)//) {
        fatal "ambiguous site name: -s $site_name or \@$1?"
            if defined($site_name) && $site_name ne $1;
        $self->site_name($1);
        shift @$argv;
        $self->usage if !@$argv;
    }
    my $cmd = shift @$argv;
### $self->command($cmd);
    $cmd =~ tr/-/_/;
    $self->command($cmd);
    goto &{ $self->can('cmd_' . $cmd) or $self->usage };
}


# --- Command handlers

sub cmd_status {
    my ($self) = @_;
    my $site = $self->orient;
    my $state = $site->state;
    print $self->json->encode($state);
}

sub cmd_login {
    my ($self) = @_;
    my ($check);
    my $site = $self->orient(
        'c|check' => \$check,
    );
    my $argv = $self->argv;
    if ($check) {
        $self->usage("login [-k USER PASSWORD]")
            if @$argv != 2;
        $self->login_if_necessary($site);
        my $result = $site->authenticate(
            'username' => $argv->[0],
            'password' => $argv->[1],
        );
        if ($result) {
            print STDERR "authentication succeeded\n";
            print STDERR "  token:  $result->{'token'}\n";
            print STDERR "  userId: $result->{'userId'}\n"
                if defined $result->{'userId'};
        }
        else {
            print STDERR "authentication failed\n";
            exit 2;
        }
    }
    else {
        $site->login('force' => 1);
        my $state = $site->state;
        printf STDERR <<'EOS', $state->{'user_id'} // '<null>', $state->{'token'};
Logged in:
  userId = %s
  token  = %s
EOS
    }
}

sub cmd_config {
    my ($self) = @_;
    my ($site, %arg) = $self->orient;
    my %config;
    if ($site) {
        %config = %{ $site->config, %$site };
        $config{'endpoint'}{'password'} =~ tr/./*/;
        delete $config{$_} for grep { ref($_) !~ /^(HASH|ARRAY)?$/ } keys %config;
    }
    my $argv = $self->argv;
    if (!@$argv) {
        print YAML::XS::Dump(\%config);
    }
    elsif (@$argv > 1) {
        $self->usage;
    }
    else {
        print YAML::XS::Dump($config{$argv->[0]});
    }
}

sub cmd_get {
    my ($self) = @_;
    my $site = $self->orient;
    my $argv = $self->argv;
    $self->usage("get URI [KEY=VAL]...") if !@$argv;
    my @args = (shift @$argv);
    if (@$argv) {
        my %arg;
        foreach (@$argv) {
            $self->usage if !/^([^=]+)=(.*)/;
            $arg{$1} = $2;
        }
        push @args, \%arg;
    }
    my $res = $site->GET(@args);
    if (!$res->is_success) {
        print STDERR $res->status_line, "\n";
        exit 2;
    }
    my $json = $self->json;
    print $json->encode($json->decode($res->content));
}

sub cmd_search {
    my ($self) = @_;
    my ($site, %arg) = $self->orient(':search');
    my $argv = $self->argv;
    $self->usage("search [-m OFFSET] [-z LIMIT] [-o ORDERBY] URI CQL") if @$argv != 2;
    my ($uri, $query) = @$argv;
    my $results = $site->search($uri, $query, %arg);
    my $json = $self->json;
    print $json->encode($results);
}

sub cmd_post {
    my ($self) = @_;
    my $site = $self->orient;
    my $argv = $self->argv;
    $self->usage("post URI JSONFILE|[KEY=VAL]...") if !@$argv;
    my $uri = shift @$argv;
    my @args;
    if (@$argv == 1 && $argv->[0] !~ /^([^=]+)=(.*)$/) {
        push @args, $self->read_json(oread(shift @$argv));
    }
    elsif (@$argv) {
        my %arg;
        foreach (@$argv) {
            $self->usage if !/^([^=]+)=(.*)$/;
            $arg{$1} = $2;
        }
        push @args, \%arg;
    }
    my $res = $site->POST($uri, @args);
    print $res->status_line, "\n";
    my $content = $res->content;
    if (defined $content && length $content) {
        if ($res->content_type eq 'application/json') {
            my $json = $self->json;
            print $json->encode($json->decode($content));
        }
        else {
            print $content;
        }
    }
}

sub cmd_put {
    my ($self) = @_;
    my $site = $self->orient;
    my $argv = $self->argv;
    $self->usage("put URI JSONFILE|[KEY=VAL]...") if !@$argv;
    my $uri = shift @$argv;
    my @args;
    if (@$argv == 1 && $argv->[0] !~ /^([^=]+)=(.*)$/) {
        push @args, $self->read_json(oread(shift @$argv));
    }
    elsif (@$argv) {
        my %arg;
        foreach (@$argv) {
            $self->usage if !/^([^=]+)=(.*)$/;
            $arg{$1} = $2;
        }
        push @args, \%arg;
    }
    my $res = $site->PUT($uri, @args);
    print $res->status_line, "\n";
    my $content = $res->content;
    if (defined $content && length $content) {
        if ($res->content_type eq 'application/json') {
            my $json = $self->json;
            print $json->encode($json->decode($content));
        }
        else {
            print $content;
        }
    }
}

sub cmd_instance {
    # usage "instance get|search|source ..."
    my ($self) = @_;
    $self->subcmd;
}

sub cmd_instance_get {
    my ($self) = @_;
    my ($with_holdings, $with_items, @properties);
    my $site = $self->orient(
        'h|with-holdings' => \$with_holdings,
        'i|with-items' => \$with_items,
        'p|properties=s' => sub { push @properties, split /\,/, $_[1] },
    );
    my $argv = $self->argv;
    $self->usage("instance get INSTANCE_ID...") if !@$argv;
    my $json = $self->json;
    foreach my $id (@$argv) {
        my $instance = $site->instance($id);
        my ($err) = split /\n/, $@;
        if (!$instance) {
            $err = ': ' if $err =~ /\S/;
            print STDERR "instance $id not found$err\n";
        }
        elsif (@properties) {
            my $propvals = $instance->properties(@properties);
            print $json->encode($propvals);
        }
        else {
            print $json->encode($instance);
        }
    }
}

sub cmd_instance_search {
    my ($self) = @_;
    my $site = $self->orient(':search');
    my $argv = $self->argv;
    $self->usage("instance search CQL") if @$argv != 1;
    my ($cql) = @$argv;
    my $srec = $site->GET("/inventory/instances", {
        'query' => $cql,
    });
    print $srec->content;
}

sub cmd_instance_source {
    my ($self) = @_;
    $self->subcmd;
}

sub cmd_instance_source_get {
    my ($self) = @_;
    my ($site, %arg) = $self->orient(
        qw(:formats),
        'g|garnish' => 'garnish',
    );
    my $argv = $self->argv;
    $self->usage("instance source get [-JMT] INSTANCE_ID...") if !@$argv;
    my $format = $arg{'format'} || FORMAT_JSON;
    foreach my $id (@$argv) {
        my $source = $site->source('instance' => $id);
        my $rectype = $source->record_type;
        if ($format eq FORMAT_MARC) {
            if ($rectype ne 'MARC') {
                print STDERR "record not in MARC format; $id\n";
                next;
            }
            my $marc = $source->{'rawRecord'}{'content'};
            if ($arg{'garnish'}) {
                print Biblio::Folio::Site::MARC->new(\$marc)->garnish(
                    'instance' => $site->instance($id),
                    'source_record' => $source,
                );
            }
            else {
                print $marc;
            }
        }
        elsif ($format eq FORMAT_TEXT) {
            if ($rectype ne 'MARC') {
                print STDERR "record not in MARC format; $id\n";
                next;
            }
            print $source->{'parsedRecord'}{'formattedContent'};
        }
        else {
            print $self->json->encode($source->{'parsedRecord'}{'content'});
        }
    }
}

sub cmd_instance_source_replace {
}

sub cmd_harvest {
    my ($self) = @_;
    my ($all, $query, $batch_size, $spell_out_locations, $use_lidb);
    my ($site, %arg) = $self->orient(
        qw(:search !offset !order-by),
        qw(:formats !as-text),
        'a|all' => \$all,
        'q|query=s' => \$query,
        'k|batch-size=i' => \$batch_size,
        'L|spell-out-locations' => \$spell_out_locations,
        'b|from-instances-database' => \$use_lidb,
        'x|include-suppressed' => 'include_suppressed',
        'S|skip-file=s' => 'skip_file',
    );
    $batch_size ||= 25;
    my $argv = $self->argv;
    $self->usage("harvest [-bL] [-k BATCHSIZE] [-a [-x] |-q CQL]")
        if @$argv && ($query || $all)
        || $query && $all;
    my @search = ('instance', '@limit' => $batch_size);
    my $bsearcher;
    my $lidb = $site->local_instances_database if $use_lidb;
    if (defined $query) {
        $bsearcher = $site->searcher(@search, '@query' => $query);
    }
    elsif ($all) {
        if ($arg{'include_suppressed'}) {
            $bsearcher = $site->searcher(@search);
        }
        else {
            $bsearcher = $site->searcher(@search, '@query' => 'discoverySuppress==false');
        }
    }
    elsif (@$argv) {
        $bsearcher = $site->searcher(@search, '@set' => $argv, '@id_field' => 'instanceId');
    }
    elsif ($lidb) {
        my $last_utc = _utc_datetime($lidb->last_sync || 0);
        $query = sprintf q{metadata.updatedDate >= "%s"}, $last_utc;
        $bsearcher = $site->searcher(@search, '@query' => $query);
    }
    else {
        $self->usage;
    }
    $site->dont_cache(qw(instance source_record holdings_record item));
    my (@bids, %bid2instance, %bid2marc, %bid2holdings);
    my %num = map { $_ => 0 } qw(instances holdings suppressed skipped nonmarc errors);
    my $verbose = $self->verbose;
    my $t0 = time;
    my $marc_fetch;
    if ($lidb) {
        $marc_fetch = sub {
            my ($instance, $instance_id) = @_;
            my $marcref = $lidb->marcref($instance_id);
            return Biblio::Folio::Site::MARC->new('marcref' => $marcref);
        };
    }
    else {
        $marc_fetch = sub {
            my ($instance, $instance_id) = @_;
            return $instance->marc_record;
        };
    }
    my %skip;
    if ($arg{'skip_file'}) {
        print STDERR "Reading skip file\n" if $verbose;
        open my $fh, '<', $arg{'skip_file'} or die "open $arg{'skip_file'}: $!";
        while (<$fh>) {
            chomp;
            $skip{$_} = 1;
        }
    }
    if ($verbose) {
        printf STDERR "\r%12s elapsed : %6s inst/sec : %8d instances : %8d holdings : %8d suppressed : %8d skipped : %8d non-MARC : %8d errors",
            '0s',
            '--',
            @num{qw(instances holdings suppressed skipped nonmarc errors)};
    }
    while (1) {
        my $instance = $bsearcher->next;
        if ($instance) {
            my $bid = $instance->id;
            $num{'instances'}++;
            $num{'skipped'}++, next if delete $skip{$bid};
            $num{'nonmarc'}++, next if $instance->{'source'} ne 'MARC';
            if ($instance->{'discoverySuppress'}) {
                $num{'suppressed'}++;
                next if !$arg{'include_suppressed'};
            }
            my $marc = eval { $marc_fetch->($instance, $bid) };
            $num{'errors'}++, next if !defined $marc;
            push @bids, $bid;
            $bid2instance{$bid} = $instance;
            $bid2marc{$bid} = $marc;
        }
        my $n = keys %bid2instance;
        if ($n && (!$instance || $n >= $batch_size)) {
# XXX Don't do this -- it's much too slow:
# my $ssearcher = $site->searcher('source_record', 'externalIdsHolder.instanceId' => \@bids, '@limit' => scalar @bids);
            my @hsearch = ('holdings_record', '@limit' => scalar @bids, 'instanceId' => \@bids);
# XXX Don't do this either -- it doesn't work
# push @hsearch, ('discoverySuppress' => JSON::false) if !$arg{'include_suppressed'};
            my $hsearcher = $site->searcher(@hsearch);
            foreach my $holdings_record ($hsearcher->all) {
                next if $holdings_record->{'discoverySuppress'} && !$arg{'include_suppressed'};
                push @{ $bid2holdings{$holdings_record->{'instanceId'}} ||= [] }, $holdings_record;
            }
            foreach my $bid (@bids) {
                my $instance = $bid2instance{$bid};
                my $holdings = $bid2holdings{$bid};
                my $marc = $bid2marc{$bid};
                # Build the MARC record
                my $ok;
                eval {
                    $marc->parse;
                    $marc->garnish('instance' => $instance);
                    if (@$holdings) {
                        $num{'holdings'} += $marc->add_holdings(
                            'holdings' => $holdings,
                            'spell_out_locations' => \$spell_out_locations,
                        );
                    }
                    else {
                        $marc->delete_holdings;
                    }
                    print $marc->as_marc21;
                    $ok = 1;
                };
                $num{'errors'}++ if !$ok;
            }
            @bids = %bid2instance = %bid2marc = %bid2holdings = ();
        }
        last if !$instance;
    }
    continue {
        if ($verbose && $num{'instances'} % 25 == 0) {
            my $dtime = time - $t0;
            printf STDERR "\r%12s elapsed : %3.2f inst/sec : %8d instances : %8d holdings : %8d suppressed : %8d skipped : %8d non-MARC : %8d errors",
                sec2dur($dtime),
                $dtime ? $num{'instances'} / $dtime : 0,
                @num{qw(instances holdings suppressed skipped nonmarc errors)};
        }
    }
    if ($verbose) {
        my $dtime = time - $t0;
        printf STDERR "\r%12s elapsed : %3.2f inst/sec : %8d instances : %8d holdings : %8d suppressed : %8d skipped : %8d non-MARC : %8d errors\n",
            sec2dur($dtime),
            $dtime ? $num{'instances'} / $dtime : 0,
            @num{qw(instances holdings suppressed skipped nonmarc errors)};
    }
}

sub cmd_holding {
    my ($self) = @_;
    $self->subcmd;
}

sub cmd_holding_get {
    my ($self) = @_;
    my $site = $self->orient;
    my $argv = $self->argv;
    $self->usage("holding get HOLDING_ID...") if !@$argv;
    my $json = $self->json;
    foreach my $id (@$argv) {
        my $irec = eval { get_holding($site, $id) };
        my ($err) = split /\n/, $@;
        if ($irec) {
            my $holding = $json->decode($irec->content);
            print $json->encode($holding);
        }
        else {
            $err = ': ' if $err =~ /\S/;
            print STDERR "holding $id not found$err\n";
        }
    }
}

sub cmd_holding_search {
    my ($self) = @_;
    my $site = $self->orient;
    my $argv = $self->argv;
    $self->usage("holding search CQL") if @$argv != 1;
    my ($cql) = @$argv;
    my $srec = $site->GET("/holdings-storage/holdings", {
        'query' => $cql,
    });
    print $srec->content;
}

sub cmd_item {
    my ($self) = @_;
    $self->subcmd;
}

sub cmd_item_get {
    my ($self) = @_;
    my $site = $self->orient;
    my $argv = $self->argv;
    $self->usage("item get ITEM_ID...") if !@$argv;
    my $json = $self->json;
    foreach my $id (@$argv) {
        my $irec = eval { get_item($site, $id) };
        my ($err) = split /\n/, $@;
        if ($irec) {
            my $item = $json->decode($irec->content);
            print $json->encode($item);
        }
        else {
            $err = ': ' if $err =~ /\S/;
            print STDERR "item $id not found$err\n";
        }
    }
}

sub cmd_item_search {
    my ($self) = @_;
    my $site = $self->orient;
    my $argv = $self->argv;
    $self->usage("item search CQL") if @$argv != 1;
    my ($cql) = @$argv;
    my $srec = $site->GET("/item-storage/items", {
        'query' => $cql,
    });
    print $srec->content;
}

sub cmd_source {
    my ($self) = @_;
    $self->subcmd;
}

sub cmd_source_get {
    my ($self) = @_;
    my ($site, %arg) = $self->orient(
        'g|garnish' => 'garnish',
        'i|for-instance' => 'for_instance',
    );
    my $argv = $self->argv;
    $self->usage("source get [-ig] ID...") if !@$argv;
    foreach my $id (@$argv) {
        my (@search, $what);
        if ($arg{'for_instance'}) {
            @search = ('instance' => $id);
            $what = "source record for instance $id";
        }
        else {
            @search = ($id);
            $what = "source record $id";
        }
        my $source = eval { $site->source(@search) };
        if (!$source) {
            print STDERR "$what not found\n";
            next;
        }
        my $format = $source->record_type;
        if (lc $format ne lc FORMAT_MARC) {
            print STDERR "$what is not in MARC format\n";
            next;
        }
        my $marc = $source->{'rawRecord'}{'content'};
        if ($arg{'garnish'}) {
            my $instance_id = $source->{'externalIdsHolder'}{'instanceId'} || $source->{'instanceId'};
            print Biblio::Folio::Site::MARC->new(\$marc)->garnish(
                'instance' => $site->instance($instance_id),
                'source_record' => $source,
            )->as_marc21;
        }
        else {
            print $marc;
        }
    }
}

sub cmd_source_search {
    my ($self) = @_;
    my $deleted;
    my ($site, %arg) = $self->orient(
        qw(:search),
        qw(:formats !as-text),
        'd|deleted' => \$deleted,
    );
    $arg{'deleted'} = JSON::true if $deleted;
    my $argv = $self->argv;
    $self->usage("source search [-m POS] [-n LIMIT] [-o KEY] [-dJM] CQL") if @$argv != 1;
    my $format = $arg{'format'} || FORMAT_JSON;
    my ($cql) = @$argv;
    my $content = $site->search('/source-storage/records', $cql, %arg);
    my ($total, $sources) = @$content{qw(totalRecords records)};
    my $json = $self->json;
    my $n = 0;
    foreach my $srec (@$sources) {
        $n++;
        my $rectype = $srec->{'recordType'};
        if ($format eq FORMAT_MARC) {
            if ($rectype ne 'MARC') {
                print STDERR "record not in MARC format: $cql [$n]\n";
                next;
            }
            my $marc = $srec->{'rawRecord'}{'content'};
            print $marc;
        }
        else {
            print $json->encode($srec->{'parsedRecord'}{'content'});
        }
    }
}

sub cmd_source_replace {
    my ($self) = @_;
    my $site = $self->orient;
    my $argv = $self->argv;
    $self->usage("source replace [MARCFILE]") if @$argv > 1;
    my $folio = $site->folio;
    my $j = _uuid();
    my $res = $site->POST("/source-storage/snapshots", {
        'jobExecutionId' => $j,
        'status' => 'NEW',
    });
    fatal "bad job?" if !$res->is_success;
    my $fh = oread(@$argv == 0 ? \*STDIN : shift @$argv);
    while (my ($marcref) = read_marc_records($fh, 1)) {
        my $id = marc_source_record_id($marcref);
        my $res = $site->PUT("/source-storage/records/$id", {
            'recordType' => 'MARC',
            'rawRecord' => {
                'id' => _uuid(),
                'content' => $$marcref,
            },
            'snapshotId' => $j,
            'matchedId' => $id,
        });
        1;
    }
    1;  # Then what?
}

sub cmd_source_sync {
    my ($self) = @_;
    my ($site, %arg) = $self->orient;
    my $argv = $self->argv;
    $self->usage("source sync DBFILE")
        if @$argv > 1;
    my ($dbfile) = @$argv;
    my $db = $site->local_instances_database($dbfile);
    my $t0 = time;
    my $total = 0;
    printf STDERR "\r%8d records fetched in %d seconds", 0, 0;
    $db->sync('progress' => sub {
        my ($n) = @_;
        $total = $n;
        printf STDERR "\r%8d records fetched in %d seconds", $n, time - $t0;
    });
    printf STDERR "\r%8d records fetched in %d seconds\n", $total, time - $t0;
}

sub cmd_source_harvest {
    # ofs=0; while (( ofs < 225222 )); do time folio @sim-fameflower get "/source-storage/records?limit=1000&offset=$ofs" > var/sources-$ofs.json; (( ofs += 1000 )); done
    my ($self) = @_;
    my ($site, %arg) = $self->orient(
        'a|all' => 'all',
        'i|instance-id-file=s' => 'instance_id_file',
        'q|query=s' => 'query',
        'k|batch-size=i' => 'batch_size',
        'b|from-instances-database=s' => 'instances_db',
    );
    my $argv = $self->argv;
    $self->usage("source harvest [-a|-i FILE|-q CQL] [-b DBFILE]")
        if @$argv || 1 < scalar grep { exists $arg{$_} } qw(all instance_id_file query);
    my $batch_size = $arg{'batch_size'} || 100;
    my $next;
    if (defined $arg{'all'}) {
        my $searcher = $site->searcher('source_record', '@limit' => $batch_size);
        $next = sub { $searcher->next };
    }
    elsif (defined $arg{'query'}) {
        my $searcher = $site->searcher('source_record', '@query' => $arg{'query'}, '@limit' => $batch_size);
        $next = sub { $searcher->next };
    }
    elsif (defined $arg{'instance_id_file'}) {
        fatal 'not yet implemented: -i FILE';
        my $f = $arg{'instance_id_file'};
        open my $fh, '<', $f or fatal "open $f: $!";
        my @instance_ids;
        $next = sub {
            while (1) {
                my $instance_id = <$fh>;
                if (defined $instance_id) {
                    chomp $instance_id;
                    push @instance_ids, $instance_id;
                }
                elsif (@instance_ids < $batch_size) {
                    next if defined $instance_id;
                }
                elsif (@instance_ids) {
                    my $searcher = $site->searcher('source_record', 'instanceId' => [@instance_ids], '@limit' => $batch_size);
                    @instance_ids = ();
                    my $instance = $searcher->next;
                    return $instance if defined $instance;
                }
                return if !defined $instance_id;
                # If we get to this point, we haven't hit the end of the file,
                # but we have just read a whole batch of instance IDs for
                # nonexistent instances -- which is totally bizarre, but
                # just barely possible
                print STDERR "warning: batch of entirely nonexistent instances\n";
            }
        };
    }
    while (1) {
        my $source_record = $next->();
        1;
    }
}

sub cmd_job {
    my ($self) = @_;
    $self->subcmd;
}

sub cmd_job_begin {
    my ($self) = @_;
    my $site = $self->orient;
    my $argv = $self->argv;
    $self->usage("job begin") if @$argv;
    my $user_id = $site->state->{'user_id'};
    my $folio = $self->folio;
    my %job = (
        'files' => [],
        'sourceType' => 'ONLINE',
        'jobProfileInfo' => {
            'id' => _uuid(),
            'name' => 'Default job profile',
            'dataType' => 'MARC'
        },
        'userId' => $user_id || _uuid(),
    );
    my $res = $site->POST('/change-manager/jobExecutions', \%job);
    my $jobexecs = $self->content($res);
    if ($jobexecs) {
        my ($jobexec, @etc) = @{ $jobexecs->{'jobExecutions'} };
        fatal "multiple jobs" if @etc;
        my $j = $jobexec->{'id'};
        print "OK $j\n";
    }
    else {
        fatal "no jobexecs returned:", $res->status_line;
    }
}

sub cmd_job_add {
    my ($self) = @_;
    # Add a file of MARC records to a job
    my $batch_size = 100;
    my $site = $self->orient(
        'k|batch-size=i' => \$batch_size,
    );
    my $argv = $self->argv;
    $self->usage("job add JOB [FILE]") if @$argv > 2 || @$argv < 1;
    my $j = shift @$argv;
    my $uri = "/change-manager/jobExecutions/$j/records";
    my $fh = oread(@$argv == 0 ? \*STDIN : shift @$argv);
    my $jfile = $site->file("var/jobs/$j.json");
    my %job = (
        'contentType' => 'MARC_RAW',
        'counter' => 0,
    );
    if (-e $jfile) {
        %job = %{ $self->read_json(oread($jfile)) };
    }
    while (1) {
        my @marcrefs = read_marc_records($fh, $batch_size);
        last if !@marcrefs;
        my @records;
        foreach (@marcrefs) {
            push @records, { 'record' => $$_ };
        }
        my $n = @marcrefs;
        my %dto = (
            'recordsMetadata' => \%job,
            'initialRecords' => \@records,
        );
        if ($n) {
            $job{'last'} = JSON::false;
            $job{'counter'} += $n;
        }
        else {
            fatal "no records to add to the job\n";
        }
        my $res = $site->POST($uri, \%dto);
        if ($res->code eq '204') {
            # Success
            print "OK $j added $n records\n";
        }
        elsif ($res->code eq '404') {
            fatal "no such job: $j";
        }
        elsif ($res->code eq '422') {
            # Validation records
            die;
        }
    }
    $self->write_json(owrite($jfile), \%job);
}

sub cmd_job_end {
    my ($self) = @_;
    my $site = $self->orient;
    my $argv = $self->argv;
    $self->usage("job end JOB") if @$argv != 1;
    my $j = shift @$argv;
    my $uri = "/change-manager/jobExecutions/$j/records";
    my $jfile = $site->file("var/jobs/$j.json");
    my %job = %{ $self->read_json(oread($jfile)) };
    $job{'last'} = JSON::true;
    $job{'total'} = $job{'counter'};
    my %dto = (
        'recordsMetadata' => \%job,
        'initialRecords' => [],
    );
    my $res = $site->POST($uri, \%dto);
    if ($res->code eq '204') {
        # Success
        print "OK $j ended\n";
        $self->write_json(owrite($jfile), \%job);
    }
    elsif ($res->code eq '404') {
        fatal "no such job: $j";
    }
    else {
        fatal "job $j not ended properly:", $res->status_line;
    }
}

sub cmd_job_status {
    my ($self) = @_;
    my $site = $self->orient;
    my $argv = $self->argv;
    $self->usage("job status JOB") if @$argv != 1;
    my ($j) = @$argv;
    my $job = $site->jobexec($j);
    1;
}

sub cmd_job_results {
    my ($self) = @_;
    my $site = $self->orient;
    my $argv = $self->argv;
    $self->usage("job results JOB") if @$argv != 1;
    my ($j) = @$argv;
    my $results = $self->content($site->GET("/metadata-provider/logs/$j"));
    my @sources = $site->objects('source', {'query' => 'snapshotId=="$j"'});
    1;
}

sub cmd_file {
    my ($self) = @_;
    $self->subcmd;
}

sub cmd_file_batch {
    my ($self) = @_;
    $self->subcmd;
}

sub cmd_file_batch_new {
    my ($self) = @_;
    my $site = $self->orient;
    my %base2path;
    my $argv = $self->argv;
    foreach my $path (@$argv) {
        my $base = basename($path);
        fatal "multiple files with the same base name: $base"
            if exists $base2path{$base};
        $base2path{$base} = $path;
    }
    my @fdefs = (
        map { { 'name' => $_ } } sort keys %base2path
    );
    my $batch = $site->create('upload_definition', {
        'fileDefinitions' => \@fdefs,
    });
    my $bid = $batch->id;
    print "OK $bid\n";
    my @files = @{ $batch->file_definitions };
    foreach my $f (@files) {
        my $fid = $f->id;
        my $base = $f->name;
        my $path = $base2path{$base}
            or fatal "wtf?!";
        print "FILE $fid $path\n";
    }
}

sub cmd_file_batch_upload {
    my ($self) = @_;
    my $site = $self->orient;
    my $argv = $self->argv;
    $self->usage if @$argv != 2;
    my $bid = shift @$argv;
    my $batch = $site->upload_definition($bid);
    my @files = @{ $batch->file_definitions };
    foreach my $path (@$argv) {
        my $base = basename($path);
        fatal "no such file in batch $bid: $base"
            if !grep { $_->name eq $base } @files;
        1;
    }
}

sub cmd_source_batch {
    my ($self) = @_;
    $self->subcmd;
}

sub cmd_source_batch_create {
    my ($self) = @_;
    my $batch_size = 500;
    my $site = $self->orient(
        'k|batch-size=i' => \$batch_size,
    );
    my $argv = $self->argv;
    my $bid = shift @$argv;
    $self->usage("source batch create [FILE]") if @$argv > 1;
    my $fh = oread(@$argv == 0 ? \*STDIN : shift @$argv);
    if (my $marcrefs = read_marc_records($fh, $batch_size)) {
        my $batch = $self->content($site->POST('/source-storage/batch/records', {
            'records' => map { $$_ } $marcrefs,
            'totalRecords' => scalar(@$marcrefs),
        }));
    }
}

sub cmd_user {
    my ($self) = @_;
    $self->subcmd;
}

sub cmd_group {
    my ($self) = @_;
    $self->subcmd;
}

sub cmd_group_search {
    my ($self) = @_;
    my ($map);
    my $site = $self->orient(
        'm' => \$map,
    );
    my $argv = $self->argv;
    $self->usage("group search CQL") if @$argv != 1;
    my ($query) = @$argv;
    my $json = $self->json;
    my $groups = eval {
        my $res = $site->GET("/groups", {
            query => $query,
        });
        $json->decode($res->content)->{'usergroups'};
    };
    if (!defined $groups) {
        fatal "group find: undefined output";
    }
    elsif ($map) {
        foreach (sort { $a->{'group'} cmp $b->{'group'} } @$groups) {
            print $_->{'id'}, ' ', $_->{'group'}, "\n";
        }
    }
    else {
        print $json->encode($groups);
    }
}

sub cmd_group_get {
    my ($self) = @_;
    my $site = $self->orient;
    my $argv = $self->argv;
    $self->usage("group get ID...") if !@$argv;
    my @groups;
    my $json = $self->json;
    foreach my $id (@$argv) {
        my $group = eval { $self->content($site->GET("/groups/$id")) };
        if (defined $group) {
            push @groups, $group;
        }
        else {
            print STDERR "no such group: $id\n";
        }
    }
    print $json->encode(\@groups) if @groups;
}

sub cmd_user_search {
    my ($self) = @_;
    my ($site, %arg) = $self->orient(':search');
    my $argv = $self->argv;
    $self->usage("user search CQL") if @$argv != 1;
    my ($query) = @$argv;
    my $users = $site->search("/users", $query, %arg);
    1;
    print $self->json->encode($users);
}

sub cmd_user_get {
    my ($self) = @_;
    my $site = $self->orient;
    my $argv = $self->argv;
    $self->usage("user get ID...") if !@$argv;
    my @users;
    foreach my $id (@$argv) {
        my $user = eval { $self->content($site->GET("/users/$id")) };
        if (defined $user) {
            push @users, $user;
        }
        else {
            print STDERR "no such user: $id\n";
        }
    }
    print $self->json->encode(\@users) if @users;
}

sub cmd_user_batch {
    my ($self) = @_;
    $self->subcmd;
}

sub cmd_user_batch_pickup {
    my ($self) = @_;
    my ($site, %arg) = $self->orient;
    my $argv = $self->argv;
    $self->usage("user batch pickup DIR") if @$argv != 1;
    $site->task('user_batch')->pickup('directory' => $argv->[0], %arg);
}

sub cmd_user_batch_validate {
    my ($self) = @_;
    my ($site, %arg) = $self->orient(
        'L|load-profile=s' => 'profile',
    );
    my $argv = $self->argv;
    $self->usage("user batch validate FILE...") if !@$argv;
    $site->task('user_batch')->validate('files' => $argv, %arg);
}

sub cmd_user_batch_parse {
    my ($self) = @_;
    my ($site, %arg) = $self->orient(
        'p|parser=s' => 'parser_cls',
        'L|load-profile=s' => 'profile',
        qw(:formats !as-marc !as-json),
    );
    my $argv = $self->argv;
    $self->usage("user batch parse [-p CLASS] [-L PROFILE] FILE...") if !@$argv;
    $site->task('user_batch')->parse('files' => $argv, %arg);
### my ($file) = @$argv;
### $arg{'site'} = $site;
### my $parser = $site->parser_for('user', $file, %arg);
### $parser->iterate(
###     %arg,
###     'each' => sub {
###         my %param = @_;
###         my $batch = $param{'batch'};
###         my ($user) = @$batch;
###         $self->print_user(%arg, 'user' => $user);
###     },
### );
}

sub cmd_user_batch_match {
    my ($self) = @_;
    my ($site, %arg) = $self->orient(
        'k|batch-size=i' => 'batch_size',
        'p|parser=s' => 'parser_cls',
        'L|load-profile=s' => 'profile',
        'x|include-rejects' => 'include_rejects',
        'y|prepare' => 'prepare',
        qw(:formats !as-marc),
    );
    $arg{'batch_size'} ||= 10;
    my $format = $arg{'format'} ||= FORMAT_JSON;
    my $argv = $self->argv;
    $self->usage("user batch match [-xy] [-k NUM] [-p CLASS] [-L PROFILE] FILE...")
        if !@$argv
        || $arg{'prepare'} && $format ne FORMAT_JSON
        ;
    $site->task('user_batch')->match('files' => $argv, %arg);
}

sub cmd_user_batch_prepare {
    my ($self) = @_;
    my ($site, %arg) = $self->orient(
        'k|batch-size=i' => 'batch_size',
        'p|parser=s' => 'parser_cls',
        'L|load-profile=s' => 'profile',
        'x|include-rejects' => 'include_rejects',
        'S|split-into=s' => 'split_into',
        'O|only-batches=s' => 'only_batches',
        qw(:formats !as-marc),
    );
    $arg{'batch_size'} ||= 10;
    my $format = $arg{'format'} ||= FORMAT_JSON;
    my $argv = $self->argv;
    $self->usage("user batch prepare [-x] [-k NUM] [-p CLASS] [-L PROFILE] FILE...")
        if !@$argv;
    $site->task('user_batch')->prepare('files' => $argv, %arg);
}

sub cmd_user_batch_load {
    my ($self) = @_;
    my ($site, %arg) = $self->orient(
        'k|batch-size=i' => 'batch_size',
        'p|parser=s' => 'parser_cls',
        'L|load-profile=s' => 'profile',
    );
    $arg{'batch_size'} ||= 10;
    my $argv = $self->argv;
    $self->usage("user batch load [-n] [-k NUM] [-p CLASS] [-L PROFILE] FILE...") if !@$argv;
    $site->task('user_batch')->load('file' => $argv, %arg);
}

sub cmd_address {
    my ($self) = @_;
    $self->subcmd;
}

sub cmd_address_types {
    my ($self) = @_;
    my $site = $self->orient;
    my $argv = $self->argv;
    $self->usage("address types") if @$argv;
    my $res = $self->content($site->GET('/addresstypes'));
    print $self->json->encode($res);
}

sub cmd_marc {
    my ($self) = @_;
    $self->subcmd;
}

sub cmd_marc_to {
    my ($self) = @_;
    $self->subcmd;
}

sub cmd_marc_to_instance {
    #@ marc to instance [@SITE] [-m MAPPING_RULES_FILE] [FILE] :: convert MARC21 records to instance data
    my ($self) = @_;
    # MARC records (as raw MARC21) ==> instances (as JSON)
    my ($mapping_file);
    my $site = $self->orient(
        'm|mapping=f' => \$mapping_file,
    );
    my $argv = $self->argv;
    $self->usage if @$argv > 1;
    my $fh = @$argv ? oread(@$argv) : \*STDIN;
    my $mapping;
    if (defined $mapping_file) {
        $mapping = $self->read_json(oread($mapping_file));
    }
    #else {
    #    $mapping = $self->content($site->get('/mapping-rules'));
    #}
    my $maker = Biblio::Folio::Site::MARC::InstanceMaker->new(
        'site' => $self->site,
        defined $mapping ? ('mapping_rules' => $mapping) : (),
    );
    $self->json_output_loop({}, 'instances', sub {
        my ($marcref) = read_marc_records($fh, 1);
        return if !defined $marcref;
        return $maker->make($marcref);
        #return $site->marc2instance($marcref, $mapping);
     });
}

sub cmd_ref {
    my ($self) = @_;
    $self->subcmd;
}

sub cmd_ref_get {
    my ($self) = @_;
    my $dir;
    my $site = $self->orient(
        'd|output-directory=s' => \$dir,
    );
    $dir ||= $site->directory('ref');
    my $argv = $self->argv;
    if (@$argv) {
        $self->usage("ref get [NAME...]") if grep { m{/} } @$argv;
        s/\.json$// for @$argv;
    }
    -d $dir or mkdir $dir or fatal "mkdir $dir: $!";
    my %data_class = Biblio::Folio::Classes->data_classes;
    my %want = map { $_ => 1 } @$argv;
    my $json = $self->json;
    my @names = sort keys %data_class;
    my $n = scalar @names;
    printf STDERR "Fetching reference data for $n type%s into $dir...\n", $n == 1 ? '' : 's';
    foreach my $name (sort keys %data_class) {
        my $cls = $data_class{$name};
        next if %want && !$want{$name};
        if ($name eq 'location-units') {
            printf STDERR "%5s %s\n", 'skip', $name;
            next;
        }
        printf STDERR "%5s %s", '', $name;
        my $file = sprintf("%s/%s.json", $dir, $name);
        my $uri = eval { $cls->_uri_search }
            || eval { (my $u = $cls->_uri) =~ s{/%s$}{}; $u }
            or fatal "\nno URL";
        my $res = $site->GET($uri);
        fatal "\nGET $uri failed: " . $res->status_line
            if !$res->is_success;
        my $content = $json->decode($res->content);
        my $fh = owrite($file);
        print $fh $json->encode($content);
        my ($objects) = grep { ref($_) eq 'ARRAY' } values %$content;
        printf STDERR "\r%5d %s\n", scalar(@$objects), $name;
    }
}

sub cmd_course {
    my ($self) = @_;
    $self->subcmd;
}

sub cmd_courses {
    my ($self) = @_;
    my $site = $self->orient;
    my @courses = $site->objects('course');
    1;
}

sub cmd_course_reserves {
    my ($self) = @_;
    my %want;
    my $batch_size = 25;
    my $site = $self->orient(
        'b|instance-ids' => \$want{'instance_ids'},
        'k|batch-size=i' => \$batch_size,
    );
    my $argv = $self->argv;
    if (@$argv) {
        fatal "not yet implemented";
    }
    else {
        my $n = 0;
        my %seen;
        while (1) {
            my @reserves = $site->objects('reserve', 'offset' => $n, 'limit' => $batch_size);
            last if !@reserves;
if (1) {
            my %item;
            foreach my $reserve (@reserves) {
                $item{$_} = 1 for $reserve->{'itemId'};
            }
            my $cql = _cql_term('id', [keys %item], 'exact' => 1, 'is_cql' => 1);
            my @items = $site->object('item', 'query' => $cql, 'limit' => $batch_size);
            $cql = _cql_term('id', [map { $_->{'holdingsRecordId'} } @items], 'exact' => 1, 'is_cql' => 1);
            my @holdings = $site->object('holdings_record', 'query' => $cql, 'limit' => $batch_size);
            if ($want{'instance_ids'}) {
                foreach my $holding (@holdings) {
                    my $bid = $holding->{'instanceId'};
                    print $bid, "\n" if !$seen{"b:$bid"}++;
                }
            }
}
if (0) {
            foreach my $reserve (@reserves) {
                my $item = $reserve->item;
                if ($want{'instance_ids'}) {
                    my $bid = eval { $item->holdings_record->{'instanceId'} };
                    if (!defined $bid) {
                        my $iid = $item->id;
                        print STDERR "warning: could not retrieve holdings record for item $iid";
                    }
                    print $bid, "\n" if !$seen{"b:$bid"}++;
                }
            }
}
            last if @reserves < $batch_size;
            $n += @reserves;
            if ($self->verbose) {
                printf STDERR "\r%8d course reserves processed", $n;
            }
        }
        if ($self->verbose) {
            printf STDERR "\r%8d course reserves processed\n", $n;
        }
    }
}

sub cmd_course_listings {
    my ($self) = @_;
    my $with_items;
    my $site = $self->orient;
    my @listings = $site->all('course_listing');
    foreach my $listing (@listings) {
        my $id = $listing->id;
        my @reserves = $listing->reserves;
        #/coursereserves/courselistings/{listing_id}/reserves
    }
}

sub cmd_course_item {
    my ($self) = @_;
    $self->subcmd;
}

sub cmd_course_item_list {
    my ($self) = @_;
    my $site = $self->orient;
    my $argv = $self->argv;
    $self->usage("course item list COURSE") if @$argv != 1;
    my ($course) = @$argv;
    1;
}

# --- Supporting functions

sub sec2dur {
    my $s = int(shift);
    my @parts;
    if ($s > 3600) {
        my $h = int($s / 3600);
        $s -= $h * 3600;
        push @parts, $h;
    }
    my $m = int($s / 60);
    $s -= $m * 60;
    push @parts, sprintf '%02d', $m;
    push @parts, sprintf '%02d', $s;
    return join(':', @parts);
}

sub json_begin_hash_array_members {
    my ($self, $hash, $key) = @_;
    my $json = $self->json;
    my $prefix = $json->encode($hash);
    my $newhash = $json->encode({$key => []});
    $prefix =~ s/,?\n}\n?\z/,\n<<$newhash>>/;
    $prefix =~ s/\,\n<<\{("[^":]+":\[)\]\s*\}>>/,\n$1/;
    return $prefix;
}

sub json_end_hash_array_members {
    return "\n]\n}\n";
}

sub json_output_loop {
    my ($self, $hash, $key, $code) = @_;
    my $json = $self->json;
    my $prefix = $self->json_begin_hash_array_members($hash, $key);
    my $n = 0;
    while (1) {
        my @next = $code->();
        last if !@next;
        $n++;
        foreach (@next) {
            print $prefix, $json->encode($_);
            $prefix = ',';
        }
    }
    print $self->json_end_hash_array_members if $n;
}

sub marc_source_record_id {
    my ($marcref) = @_;
    my ($leader, $fields) = marcparse($marcref, 'only' => {'999' => 1});
    foreach my $field (@$fields) {
        my $valref = $field->[VALREF];
        return $1 if $$valref =~ /\x1fs([^\s\x1d-\x1f]+)/;
    }
    return;
}

sub skip_marc_records {
    my ($fh, $n) = @_;
    my $n0 = $n;
    while ($n--) {
        local $/ = "\x1d";
        my $marc = <$fh>;
        last if !defined $marc;
    }
    return $n - $n0;
}

sub read_marc_records {
    my ($fh, $n) = @_;
    my @records;
    $n ||= 1000;
    while ($n--) {
        local $/ = "\x1d";
        my $marc = <$fh>;
        last if !defined $marc;
        push @records, \$marc;
    }
    return wantarray ? @records : \@records;
}

sub read_json {
    my ($self, $fh) = @_;
    local $/;
    my $str = <$fh>;
    return if !defined $str;
    return $self->json->decode($str);
}

sub write_json {
    my ($self, $fh, $data) = @_;
    print $fh $self->json->encode($data);
}

sub oread {
    my ($f) = @_;
    my $fh;
    if ($f eq '-' || $f eq '/dev/stdin') {
        return \*STDIN;
    }
    elsif ($f =~ /\.gz/) {
        open $fh, '-|', 'gunzip', '--stdout', $f
            or fatal "exec gunzip $f: $!";
    }
    else {
        open $fh, '<', $f
            or fatal "open $f: $!";
    }
    return $fh;
}

sub owrite {
    my ($f) = @_;
    my $fh;
    if ($f eq '-' || $f eq '/dev/stdout') {
        return \*STDOUT;
    }
    elsif ($f =~ /\.gz/) {
        open $fh, "|gzip > $f"
            or fatal "exec gzip > $f: $!";
    }
    else {
        open $fh, '>', $f
            or fatal "open $f for writing: $!";
    }
    return $fh;
}

sub content {
    my ($self, $res) = @_;
    die "no response" if !defined $res;
    my $str = $res->content;
    die "no content" if !defined $str;
    my $content = eval { $self->json->decode($str) }
        or die "unparseable content";
    return $content;
}

sub subcmd {
    my ($self) = @_;
    my $argv = $self->argv;
    $self->usage if !@$argv;
    my $subcmd = shift @$argv;
    my @caller = caller 1;
    $caller[3] =~ /(cmd_\w+)$/ or die;
    goto &{ $self->can($1.'_'.$subcmd) or $self->usage };
}

sub get_holding {
    my ($site, $id) = @_;
    return $site->GET("/holdings-storage/holdings/$id");
}

sub get_item {
    my ($site, $id) = @_;
    return $site->GET("/item-storage/items/$id");
}

sub login_if_necessary {
    my ($self, $site) = @_;
    my $token = $site->token;
    $site->login('reuse_token' => 1);
    return 0 if defined $token && $token eq $site->token;
    hook('login');
    return 1;
}

sub _indent {
    my $str = shift;
    my $indent = ' ' x (@_ ? pop : 4);
    return join '', map { $indent . $_ . "\n" } split /\n/, $str;
}

sub _marc_delete_stub {
    my ($id) = @_;
    my $leader = '00000dam a2200000 a 4500';
    my @fields = ( marcfield('001', $id) );
    return marcbuild($leader, \@fields);
}

sub print_cooked_marc {
    my %arg = @_;
    my $marcref = $arg{'marcref'} || \$arg{'marc'};
    my %sub = ('i' => $arg{'instance_id'}, 's' => $arg{'source_record_id'});
    my @s999;
    my %set;
    foreach (sort keys %sub) {
        $set{$_} = 1;
        push @s999, $_ => $sub{$_};
    }
    my $dirty;
    my ($leader, $fields);
    if (keys %set) {
        ($leader, $fields) = marcparse($marcref);
        $dirty = 1;
        my ($f999) = grep {
            $_->[TAG] eq '999' &&
            $_->[IND1] eq 'f'  &&
            $_->[IND2] eq 'f'
        } @$fields;
        if ($f999) {
            my $valref = $f999->[VALREF];
            while ($$valref =~ /\x1f([is])([^\x1d-\x1f]+)/g) {
                my $val = $sub{$_} or next;
                delete $set{$1} if $2 eq $val;
            }
            $f999->[DELETE] = $dirty = keys %set;
        }
    }
    elsif ($arg{'strict'}) {
        die "no instance or source record IDs to insert into MARC record"
    }
    if ($dirty) {
        push @$fields, marcfield('999', 'f', 'f', @s999);
        print marcbuild($leader, $fields);
    }
    else {
        print $$marcref;
    }
}

sub hook {
    my $what = shift;
    my $sub = __PACKAGE__->can('hook_'.$what)
        or return;
    $sub->(@_);
}

sub hook_login {
    1;
}

sub orient {
    my $self = shift;
    my $root = $self->root;
    my $site_name = $self->site_name;
    my @specs = (':standard', @_);
    my %arg;
    my %tracing;
    my %optgroup = (
        'standard' => {
            'r|root=s' => \$root,
            's|site=s' => \$site_name,
            'v|verbose' => \$self->{'verbose'},
            'n|dry-run' => \$self->{'dryrun'},
        },
        'search' => {
            'm|offset=s' => \$arg{'offset'},
            'z|limit=i' => \$arg{'limit'},
            'o|order-by=s' => \$arg{'order_by'},
        },
        'formats' => {
            'J|as-json' => sub { $arg{'format'} = FORMAT_JSON },
            'M|as-marc' => sub { $arg{'format'} = FORMAT_MARC },
            'T|as-text' => sub { $arg{'format'} = FORMAT_TEXT },
        },
        'debugging' => {
            'X|tracing=s' => sub {
                $self->_parse_tracing(\%tracing, $_[1]);
            },
        },
    );
    my %opt;
    my %not;
    while (@specs) {
        my %o = parse_opt_spec(shift @specs);
        my $name = $o{'name'};
        if ($o{'is_group'}) {
            my $opts = $optgroup{$name} || fatal "unrecognized option group: $name";
            push @specs, %$opts;
        }
        elsif ($o{'is_negator'}) {
            $not{$name} = 1;
        }
        elsif (@specs) {
            my $dest = shift @specs;
            if (!ref $dest) {
                my $key = $dest;
                $dest = \$arg{$key};
            }
            $opt{$o{'spec'}} = $dest;
        }
        else {
            $self->usage;
        }
    }
    foreach my $not (keys %not) {
        $not =~ s/[:=].+$//;
        my @keys = split /\|/, $not;
        foreach my $key ($not, @keys) {
            last if defined delete $opt{$key};
        }
    }
    local $SIG{__WARN__} = sub {
        print @_ if $_[0] !~ /^Unknown option/;
    };
    my $argv = $self->argv;
    GetOptionsFromArray($argv, %opt) or $self->usage;
    my $json = JSON->new->pretty->canonical->convert_blessed;
    my $folio = Biblio::Folio->new('root' => $root, 'json' => $json);
    $self->json($json);
    $self->folio($folio);
    my $site;
    if (defined $site_name) {
        if (-l "$root/site/$site_name") {
            $site_name = readlink "$root/site/$site_name"
                or die "readlink $root/site/$site_name: $!";
            $site_name =~ s{.*/}{};
        }
        $site = $folio->site(
            $site_name,
            keys(%tracing) ? ('_tracing' => \%tracing) : (),
        );
        $self->site($site);
    }
    my $cmd = $self->command;
    if ($site && $cmd ne 'login') {
        $self->login_if_necessary($site);
        # print STDERR "Reusing login for $site_name\n"
        #     if !login_if_necessary($site);
    }
    if (wantarray) {
        # list context
        $DB::single = 1;
        return ($site, %arg);
    }
    else {
        # scalar or void context
        $self->usage if grep { defined $arg{$_} } keys %arg;
        $DB::single = 1;
        return $site;
    }
}

sub parse_opt_spec {
    local $_ = shift;
    return (
        'is_group' => 1,
        'name' => $1,
    ) if /^:(\S+)/;
    return (
        'is_negator' => 1,
        'name' => $1,
    ) if /^!(.+)$/;
    return (
        'is_auto' => 1,
        'spec' => $1,
        'name' => $2,
    ) if /^(\S+)\s+(\S+)$/;
    return ('spec' => $_);
}

sub _parse_tracing {
    my ($self, $tracing, $str) = @_;
    $tracing->{'state'} = 'ON';
    foreach (split /:/, $str) {
        $tracing->{'file'} = $1, next if m{^file=(.+)$};
        $tracing->{'file'} = '/dev/stderr', next if $_ eq 'stderr';
        if (s/^show=//) {
            foreach (split /,/) {
                my $bool = s/^!// ? 0 : 1;
                $tracing->{'show_'.$_} = $bool;
            }
        }
    }
}

sub fatal {
    if (@_ && ref($_[0]) && $_[0]->isa(__PACKAGE__) && defined $_[0]{'fatal'}) {
        goto &{ $_[0]{'fatal'} };
    }
    my $prog = _program_name(@_);
    print STDERR "$prog: @_\n";
    exit 2;
}

sub usage {
    my @args = @_;
    my $self;
    if (@_ && ref($_[0]) && $_[0]->isa(__PACKAGE__)) {
        $self = shift;
        goto &{ $self->{'usage'} } if defined $self->{'usage'};
    }
    my $prog = _program_name(@args);
    my $usage;
    my @commands;
    my %have_usage;
    my $cmd;
    foreach my $i (1..3) {
        my @caller = caller($i);
        last if !@caller;
        $cmd = $caller[3];
        last if $cmd =~ s/.+::cmd_//;
        undef $cmd;
    }
    if (@_) {
        $usage = defined($cmd) ? '@SITE ' . shift : shift;
    }
    if (defined $cmd && open my $fh, '<', __FILE__) {
        my $incmd;
        while (<$fh>) {
            if (/^sub cmd_(\S+)/) {
                $incmd = $1;
            }
            elsif (/^\}/) {
                undef($incmd);
            }
            elsif (!defined $incmd) {
                next;
            }
            elsif (m{->usage\("([^"]+)"} || m{ +usage +"([^"]+)"}) {
                (my $shortcmd = $incmd) =~ s/_.*//;
                push @commands, $shortcmd
                    if !$have_usage{$shortcmd}++;
                $usage = '@SITE ' . $1
                    if defined $incmd && $incmd eq $cmd;
            }
        }
    }
    $usage ||= '@SITE COMMAND [ARG...]';
    print STDERR "usage: $prog $usage\n";
    print STDERR "commands:\n" if @commands;
    print STDERR '  ', $_, "\n" for sort @commands;
    exit 1;
}

sub _program_name {
    (my $prog = _program_file(@_)) =~ s{.*/}{};
    return $prog;
}

sub _program_file {
    return shift()->program_file if @_;
    return $0;
}

sub dd {
    my @lines;
    if (@_ >= 1 && eval { $_[0]->can('_uri') }) {
        my $obj = shift;
        my %obj = map {
            /^_/ ? ()
                 : ($_ => $obj->{$_})
        } keys %$obj;
        @lines = split /(?<=\n)/, Dump(bless \%obj, ref $obj);
    }
    else {
        @lines = split /(?<=\n)/, Dump(@_);
    }
    if (@lines > 50 && open my $fh, '|-', 'less') {
        print $fh @lines;
    }
    else {
        print @lines;
    }
}

1;
