package Biblio::Folio::App;

# A FOLIO application

use strict;
use warnings;

use Biblio::Folio;
use Biblio::Folio::Site::MARC;
use Biblio::Folio::Classes;
use Biblio::Folio::Util qw(_make_hooks _optional);

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

my $prog;

sub new {
    my $cls = shift;
    my $self = bless { @_ }, $cls;
    $self->init;
    $prog = $self->program;
    return $self;
}

sub root { @_ > 1 ? $_[0]{'root'} = $_[1] : $_[0]{'root'} }
sub site_name { @_ > 1 ? $_[0]{'site_name'} = $_[1] : $_[0]{'site_name'} }

sub folio { @_ > 1 ? $_[0]{'folio'} = $_[1] : $_[0]{'folio'} }
sub site { @_ > 1 ? $_[0]{'site'} = $_[1] : $_[0]{'site'} }
sub json { @_ > 1 ? $_[0]{'json'} = $_[1] : $_[0]{'json'} }

sub argv { @_ > 1 ? $_[0]{'argv'} = $_[1] : $_[0]{'argv'} }
sub program { @_ > 1 ? $_[0]{'program'} = $_[1] : $_[0]{'program'} }
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
    usage if !@$argv;
    if ($argv->[0] =~ s/^\@(.+)//) {
        fatal "ambiguous site name: -s $site_name or \@$1?"
            if defined($site_name) && $site_name ne $1;
        $self->site_name($1);
        shift @$argv;
        usage if !@$argv;
    }
    my $cmd = shift @$argv;
### $self->command($cmd);
    $cmd =~ tr/-/_/;
    $self->command($cmd);
    goto &{ $self->can('cmd_' . $cmd) or usage };
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
        usage "login [-k USER PASSWORD]"
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

sub cmd_get {
    my ($self) = @_;
    my $site = $self->orient;
    my $argv = $self->argv;
    usage "get URI [KEY=VAL]..." if !@$argv;
    my @args = (shift @$argv);
    if (@$argv) {
        my %arg;
        foreach (@$argv) {
            usage if !/^([^=]+)=(.*)/;
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
    my ($site, %arg) = $self->orient(':fetch');
    my $argv = $self->argv;
    usage "search [-m OFFSET] [-z LIMIT] [-o ORDERBY] URI CQL" if @$argv != 2;
    my ($uri, $query) = @$argv;
    my $results = $site->search($uri, $query, %arg);
    my $json = $self->json;
    print $json->encode($results);
}

sub cmd_post {
    my ($self) = @_;
    my $site = $self->orient;
    my $argv = $self->argv;
    usage "post URI JSONFILE|[KEY=VAL]..." if !@$argv;
    my $uri = shift @$argv;
    my @args;
    if (@$argv == 1 && $argv->[0] !~ /^([^=]+)=(.*)$/) {
        push @args, $self->read_json(oread(shift @$argv));
    }
    elsif (@$argv) {
        my %arg;
        foreach (@$argv) {
            usage if !/^([^=]+)=(.*)$/;
            $arg{$1} = $2;
        }
        push @args, \%arg;
    }
    my $res = $site->POST($uri, @args);
    my $json = $self->json;
    print $json->encode($json->decode($res->content));
}

sub cmd_put {
    my ($self) = @_;
    my $site = $self->orient;
    my $argv = $self->argv;
    usage "put URI JSONFILE|[KEY=VAL]..." if !@$argv;
    my $uri = shift @$argv;
    my @args;
    if (@$argv == 1 && $argv->[0] !~ /^([^=]+)=(.*)$/) {
        push @args, $self->read_json(oread(shift @$argv));
    }
    elsif (@$argv) {
        my %arg;
        foreach (@$argv) {
            usage if !/^([^=]+)=(.*)$/;
            $arg{$1} = $2;
        }
        push @args, \%arg;
    }
    my $res = $site->PUT($uri, @args);
    my $json = $self->json;
    print $json->encode($json->decode($res->content));
}

sub cmd_instance {
    my ($self) = @_;
    $self->subcmd();
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
    usage "instance get INSTANCE_ID..." if !@$argv;
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
    my $site = $self->orient;
    my $argv = $self->argv;
    usage "instance search CQL" if @$argv != 1;
    my ($cql) = @$argv;
    my $srec = $site->GET("/inventory/instances", {
        'query' => $cql,
    });
    print $srec->content;
}

sub cmd_instance_source {
    my ($self) = @_;
    $self->subcmd();
}

sub cmd_instance_source_get {
    my ($self) = @_;
    my ($site, %arg) = $self->orient(qw(:formats));
    my $argv = $self->argv;
    usage "instance source get [-JMT] INSTANCE_ID..." if !@$argv;
    my %format = %{ $arg{'format'} };
    usage if keys(%format) > 1;
    $format{'json'} = 1 if keys(%format) == 0;
    foreach my $id (@$argv) {
        my $source = $site->source('instance' => $id);
        my $rectype = $source->record_type;
        if ($format{'marc'}) {
            if ($rectype ne 'MARC') {
                print STDERR "record not in MARC format; $id\n";
                next;
            }
            my $marc = $source->{'rawRecord'}{'content'};
            my $sid = $source->id;
            print_cooked_marc(
                'marcref' => \$marc,
                'instance_id' => $id,
                'source_id' => $sid,
                'strict' => 1,
            );
            #my %opt = (
            #    'error' => sub { die @_ },
            #);
            #my ($leader, $fields, $marcref) = marcparse(\$marc, \%opt);
            #1;
            #print $marc;
        }
        elsif ($format{'text'}) {
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

sub cmd_instance_harvest {
    my ($self) = @_;
    my (%with, $mod, $id_file);
    my ($query, $batch_size, $max_err, $spell_out_locations);
    my ($site, %arg) = $self->orient(
        qw(:fetch !offset !order-by),
        qw(:formats !as-text),
        'h|with-holdings' => \$with{'holdings'},
        'i|with-items'    => \$with{'items'},
        'k|batch-size=i' => \$batch_size,
        'e|max-errors=i' => \$max_err,
        'p|progress=i' => \$mod,
        'f|id-file=s' => \$id_file,
        'q|query=s' => \$query,
        'L|spell-out-locations' => \$spell_out_locations,
    );
    my $max_count = $arg{'limit'};
    my $argv = $self->argv;
    usage "instance harvest [-jmhi] [-n MAXNUM] [-k BATCHSIZE] [-e MAXERRS] [-f FILE|-q CQL|ID...]"
        if ($id_file && $query)
        || ($id_file && @$argv)
        || ($query   && @$argv)
        ;
    my %as = %{ $arg{'format'} || { 'marc' => 1 } };
    $with{'holdings'} = 1 if $with{'items'};
    my $t0 = time;
    my $err = 0;
    my $remaining = $max_count || (1<<31);
    my $fetch;
    my $prefix = qq{\{"instances":[\n};
    if (defined $query) {
        my $offset = 0;
        if (!defined $batch_size) {
            $batch_size = (!defined $max_count || $max_count > 100) ? 100 : $max_count;
        }
        $fetch = sub {
            my $limit = $batch_size > $remaining ? $remaining : $batch_size;
            my @instances = $site->instance(
                'query' => $query,
                'offset' => $offset,
                'limit' => $limit,
            );
            my $n = scalar @instances;
            $remaining = 0, return if !$n;
            $remaining -= $n;
            $offset += $n;
            return @instances;
        };
    }
    else {
        my $next;
        if (@$argv) {
            $next = sub {
                return if !@$argv;
                return shift @$argv;
            };
        }
        else {
            my $fh = @$argv ? oread(@$argv) : \*STDIN;
            $next = sub {
                my $id = <$fh>;
                return if !defined $id;
                $id =~ s/\s+.*//;
                return $id;
            };
        }
        $fetch = sub {
            while (1) {
                my $id = $next->();
                $remaining = 0, return if !defined $id;
                my (@instance, $ok);
                eval { @instance = $site->instance($id); $ok = 1 };
                if (!$ok) {
                    my ($msg) = split /\n/, $@;
                    $msg = 'unknown error' if $msg !~ /\S/;
                    print STDERR "can't fetch instance $id: $msg\n";
                    $err++;
                }
                elsif (@instance) {
                    $remaining -= @instance;
                    return @instance;
                }
            }
        };
    }
    my %num = qw(instances 0 holdings 0 items 0);
    my $n = 0;
    while (1) {
        my @instances = $fetch->();
        last if !@instances;
        foreach my $instance (@instances) {
            $num{'instances'}++;
            $n++;
            my $id = $instance->id;
            my $hrid = $instance->hrid;
            if ($as{'marc'}) {
                my $marc;
                eval {
                    $marc = $instance->marc_record;
                    my ($leader, $fields) = ($marc->leader, $marc->fields);
                    @$fields = (
                        marcfield('001', $id),
                        ( grep { $_->[TAG] !~ /^(001|003|852|859|9)/ } @$fields ),
                    );
# XXX This whole hack with $classifier is crazy -- it should be done in a separate, post-processing script (or plugin)
                    my $classifier;
                    my $f33x = _33x_field_subfields($fields);
                    $classifier = 'audiobook' if $f33x->{'336'}{'spw'};
                    $classifier = 'ebook' if $f33x->{'336'}{'txt'} && $f33x->{'338'}{'cr'};
# END crazy hack
                    if ($with{'holdings'}) {
                        my @holdings = $instance->holdings;
                        my $num_holdings = @holdings;
                        if ($num_holdings) {
                            my $num_items = add_holdings_to_marc_fields(
                                'fields' => $fields,
                                'holdings' => \@holdings,
                                'add_items' => $with{'items'},
                                'classifier' => $classifier,
                                'spell_out_locations' => \$spell_out_locations,
                            );
                            $num{'holdings'} += $num_holdings;
                            $num{'items'} += $num_items;
                            $n += $num_holdings + $num_items;
                        }
                    }
                    push @$fields, marcfield('901', ' ', ' ', 'h' => $hrid);
                    $marc = marcbuild($leader, $fields);
                };
                if (!defined $marc) {
                    $marc = Biblio::Folio::Site::MARC->stub('instance' => $instance, 'status' => 'd');
                    print STDERR "not found: $id\n"
                        if $self->verbose;
                }
                print $marc;
            }
            elsif ($as{'json'}) {
                my $json = $self->json;
                if ($with{'holdings'}) {
                    my @holdings = $instance->holdings;
                    $instance->{'holdings'} = \@holdings;
                    my $num_holdings = @holdings;
                    if ($with{'items'}) {
                        foreach my $holding (@holdings) {
                            my @items = $holding->items;
                            $holding->{'items'} = \@items;
                            $num{'items'} += @items;
                            $n += @items;
                        }
                    }
                    $num{'holdings'} += @holdings;
                    $n += @holdings;
                }
                my $out = $json->encode($instance);
                $out =~ s/\n+\z//;
                print $prefix, $out;
                $prefix = ',';
            }
        }
        if ($self->verbose && defined $query || $mod && ($n % $mod) == 0) {
            printf STDERR "\r%8d instances : %8d holdings : %8d items",
                @num{qw(instances holdings items)};
        }
    }
    print "\n]\n}\n" if $as{'json'};
    if ($self->verbose && defined $query || $mod) {
        printf STDERR "\r%8d instances : %8d holdings : %8d items => %.1f seconds\n",
            @num{qw(instances holdings items)},
            time - $t0;
    }
}

sub cmd_holding {
    my ($self) = @_;
    $self->subcmd();
}

sub cmd_holding_get {
    my ($self) = @_;
    my $site = $self->orient;
    my $argv = $self->argv;
    usage "holding get HOLDING_ID..." if !@$argv;
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
    usage "holding search CQL" if @$argv != 1;
    my ($cql) = @$argv;
    my $srec = $site->GET("/holdings-storage/holdings", {
        'query' => $cql,
    });
    print $srec->content;
}

sub cmd_item {
    my ($self) = @_;
    $self->subcmd();
}

sub cmd_item_get {
    my ($self) = @_;
    my $site = $self->orient;
    my $argv = $self->argv;
    usage "item get ITEM_ID..." if !@$argv;
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
    usage "item search CQL" if @$argv != 1;
    my ($cql) = @$argv;
    my $srec = $site->GET("/item-storage/items", {
        'query' => $cql,
    });
    print $srec->content;
}

sub cmd_source {
    my ($self) = @_;
    $self->subcmd();
}

sub cmd_source_get {
    my ($self) = @_;
    my $site = $self->orient;
    my $argv = $self->argv;
    usage "source get SOURCE_RECORD_ID..." if !@$argv;
    my $json = $self->json;
    foreach my $id (@$argv) {
        my $srec = eval { $json->decode($site->GET("/source-storage/records/$id")->content) };
        my ($err) = split /\n/, $@;
        if (defined $srec) {
            my $instance_id = $srec->{'externalIdsHolder'}{'instanceId'} || $srec->{'instanceId'};
            print_cooked_marc(
                'marcref' => \$srec->{'rawRecord'}{'content'}, 
                'instance_id' => $instance_id,
                'source_id' => $id,
                'strict' => 1,
            );
        }
        else {
            $err = ': ' if $err =~ /\S/;
            print STDERR "record $id not found$err\n";
        }
    }
}

sub cmd_source_search {
    my ($self) = @_;
    my $deleted;
    my ($site, %arg) = $self->orient(
        qw(:fetch),
        qw(:formats !as-text),
        'd|deleted' => \$deleted,
    );
    $arg{'deleted'} = JSON::true if $deleted;
    my $argv = $self->argv;
    usage "source search [-m POS] [-n LIMIT] [-o KEY] [-dJM] CQL" if @$argv != 1;
    my %format = %{ $arg{'format'} || { 'json' => 1 } };
    my ($cql) = @$argv;
    my $content = $site->search('/source-storage/records', $cql, %arg);
    my ($total, $sources) = @$content{qw(totalRecords records)};
    my $json = $self->json;
    my $n = 0;
    foreach my $srec (@$sources) {
        $n++;
        my $rectype = $srec->{'recordType'};
        if ($format{'marc'}) {
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
    usage "source replace [MARCFILE]" if @$argv > 1;
    my $folio = $site->folio;
    my $j = $folio->uuid;
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
                'id' => $folio->uuid,
                'content' => $$marcref,
            },
            'snapshotId' => $j,
            'matchedId' => $id,
        });
        1;
    }
    1;  # Then what?
}

sub cmd_job {
    my ($self) = @_;
    $self->subcmd();
}

sub cmd_job_begin {
    my ($self) = @_;
    my $site = $self->orient;
    my $argv = $self->argv;
    usage "job begin" if @$argv;
    my $user_id = $site->state->{'user_id'};
    my $folio = $self->folio;
    my %job = (
        'files' => [],
        'sourceType' => 'ONLINE',
        'jobProfileInfo' => {
            'id' => $folio->uuid,
            'name' => 'Default job profile',
            'dataType' => 'MARC'
        },
        'userId' => $user_id || $folio->uuid,
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
    usage "job add JOB [FILE]" if @$argv > 2 || @$argv < 1;
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
    usage "job end JOB" if @$argv != 1;
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
    usage "job status JOB" if @$argv != 1;
    my ($j) = @$argv;
    my $job = $site->jobexec($j);
    1;
}

sub cmd_job_results {
    my ($self) = @_;
    my $site = $self->orient;
    my $argv = $self->argv;
    usage "job results JOB" if @$argv != 1;
    my ($j) = @$argv;
    my $results = $self->content($site->GET("/metadata-provider/logs/$j"));
    my @sources = $site->objects('source', {'query' => 'snapshotId=="$j"'});
    1;
}

sub cmd_file {
    my ($self) = @_;
    $self->subcmd();
}

sub cmd_file_batch {
    my ($self) = @_;
    $self->subcmd();
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
    usage if @$argv != 2;
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
    $self->subcmd();
}

sub cmd_source_batch_create {
    my ($self) = @_;
    my $batch_size = 500;
    my $site = $self->orient(
        'k|batch-size=i' => \$batch_size,
    );
    my $argv = $self->argv;
    my $bid = shift @$argv;
    usage "source batch create [FILE]" if @$argv > 1;
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
    $self->subcmd();
}

sub cmd_group {
    my ($self) = @_;
    $self->subcmd();
}

sub cmd_group_search {
    my ($self) = @_;
    my ($map);
    my $site = $self->orient(
        'm' => \$map,
    );
    my $argv = $self->argv;
    usage "group search CQL" if @$argv != 1;
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
    usage "group get ID..." if !@$argv;
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
    my ($site, %arg) = $self->orient(':fetch');
    my $argv = $self->argv;
    usage "user search CQL" if @$argv != 1;
    my ($query) = @$argv;
    my $users = $site->search("/users", $query, %arg);
    1;
    print $self->json->encode($users);
}

sub cmd_user_get {
    my ($self) = @_;
    my $site = $self->orient;
    my $argv = $self->argv;
    usage "user get ID..." if !@$argv;
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

sub cmd_user_parse {
    my ($self) = @_;
    my ($site, %arg) = $self->orient(
        'p|parser=s' => 'parser_cls',
        'L|load-profile=s' => 'profile',
        qw(:formats !as-marc !as-json),
    );
    my $argv = $self->argv;
    usage "user match [-p CLASS] [-L PROFILE] FILE" if @$argv != 1;
    my ($file) = @$argv;
    $arg{'site'} = $site;
    $site->process_file('user', $file, %arg, 'each' => sub {
        my %param = @_;
        my $batch = $param{'batch'};
        my ($user) = @$batch;
        print_user(%arg, 'user' => $user);
    });
}

sub cmd_user_match {
    my ($self) = @_;
    my ($site, %arg) = $self->orient(
        'k|batch-size=i' => 'batch_size',
        'p|parser=s' => 'parser_cls',
        'L|load-profile=s' => 'profile',
        'x|include-rejects' => 'include_rejects',
        qw(:formats !as-marc),
    );
    my $argv = $self->argv;
    usage "user match [-s NUM] [-p CLASS] [-L PROFILE] FILE" if @$argv != 1;
    my ($file) = @$argv;
    my $matcher = $site->matcher('user', %arg);
    $arg{'site'} = $site;
    $arg{'batch_size'} ||= 10;
    my $batch_base = 1;
    $site->process_file('user', $file, %arg, 'each' => sub {
        my %param = @_;
        my $batch = $param{'batch'};
        my $results = $matcher->match(@$batch);
        $self->show_matching_users(%param, 'batch_base' => $batch_base, 'results' => $results);
        $batch_base += $arg{'batch_size'};
    });
}

sub cmd_user_load {
    my ($self) = @_;
    my ($site, %arg) = $self->orient(
        'k|batch-size=i' => 'batch_size',
        'p|parser=s' => 'parser_cls',
        'L|load-profile=s' => 'profile',
    );
    my $argv = $self->argv;
    usage "user load [-n] [-s NUM] [-p CLASS] [-L PROFILE] FILE" if @$argv != 1;
    my ($file) = @$argv;
    my $matcher = $site->matcher('user', %arg);
    $arg{'site'} = $site;
    $arg{'batch_size'} ||= 10;
    $site->process_file('user', $file, %arg, 'each' => sub {
        my %param = @_;
        my $batch = $param{'batch'};
        my @results = $matcher->match(@$batch);
        foreach my $result (@results) {
            $self->update_or_create_user(%param, 'result' => $result);
        }
    });
}

sub cmd_address {
    my ($self) = @_;
    $self->subcmd();
}

sub cmd_address_types {
    my ($self) = @_;
    my $site = $self->orient;
    my $argv = $self->argv;
    usage "address types" if @$argv;
    my $res = $self->content($site->GET('/addresstypes'));
    print $self->json->encode($res);
}

sub cmd_marc {
    my ($self) = @_;
    $self->subcmd();
}

sub cmd_marc_to {
    my ($self) = @_;
    $self->subcmd();
}

sub cmd_marc_to_instance {
    my ($self) = @_;
    # MARC records (as raw MARC21) ==> instances (as JSON)
    my ($mapping_file);
    my $site = $self->orient(
        'm|mapping=f' => \$mapping_file,
    );
    my $argv = $self->argv;
    usage if @$argv > 1;
    my $fh = @$argv ? oread(@$argv) : \*STDIN;
    my $mapping;
    if (defined $mapping_file) {
        $mapping = $self->read_json(oread($mapping_file));
    }
    else {
        $mapping = $self->content($site->get('/mapping-rules'));
    }
    $self->json_output_loop({}, 'instances', sub {
        my ($marcref) = read_marc_records($fh, 1);
        return if !defined $marcref;
        return $site->marc2instance($marcref, $mapping);
    });
}

sub cmd_ref {
    my ($self) = @_;
    $self->subcmd();
}

sub cmd_ref_get {
    my ($self) = @_;
    my $dir;
    my $site = $self->orient(
        'd|output-directory=s' => \$dir,
    );
    $dir ||= $site->dir('ref');
    my $argv = $self->argv;
    if (@$argv) {
        usage "ref get [NAME...]" if grep { m{/} } @$argv;
        s/\.json$// for @$argv;
    }
    -d $dir or mkdir $dir or fatal "mkdir $dir: $!";
    my %datafile = Biblio::Folio::Classes->datafiles;
    my %want = map { $_ => 1 } @$argv;
    my $json = $self->json;
    foreach my $cls (sort keys %datafile) {
        my $name = $datafile{$cls};
        next if %want && !$want{$name};
        my $file = sprintf("%s/%s.json", $dir, $name);
        my @objects = $cls->_all;
        my $fh = owrite($file);
        print $fh $json->encode(\@objects);
    }
}

# --- Supporting functions

sub json_output_loop {
    my ($self, $hash, $key, $code) = @_;
    my $json = $self->json;
    my $prefix = $json->encode($hash);
    my $newhash = $json->encode({$key => []});
    $prefix =~ s/,?\n}\n?\z/,\n<<$newhash>>/;
    $prefix =~ s/\,\n<<\{("[^":]+":\[)\]\s*\}>>/,\n$1/;
    while (1) {
        my @next = $code->();
        last if !@next;
        foreach (@next) {
            print $prefix, $json->encode($_);
            $prefix = ',';
        }
    }
    print "\n]\n}\n";
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

### sub userfile_parser {
###     my %arg = @_;
###     my $site = $arg{'site'};
###     my $profile = $site->load_profile('user', $arg{'profile'});
###     my %parser = %{ $profile->{'parser'} };
###     my $parser_cls = $parser{'class'} || 'Biblio::FolioX::Util::JSONParser';
###     $parser_cls = 'Biblio::FolioX' . $parser_cls if $parser_cls =~ /^[+]/;
###     delete $parser{'class'};
###     use_class($parser_cls);
###     return $parser_cls->new('site' => $site, %parser, 'file' => $arg{'file'});
### }
### 
### sub proc_userfile {
###     my ($site, %arg) = @_;
###     # TODO
###     if (0 && defined $arg{'format'}) {
###         my $formatter = output_formatter($arg{'format'});
###         %arg = _make_hooks(
###             %arg,
###             'before' => sub { $formatter->before },
###             'after' => sub { $formatter->after },
###         );
###     }
###     my $parser = userfile_parser(%arg);
###     $parser->iterate(%arg);
### }
### 
### sub match_or_load_users {
###     my ($verb, %arg) = @_;
###     my $site = $arg{'site'};
###     my $parser = userfile_parser(%arg);
###     my ($batch_size, $parser_cls) = @arg{qw(batch_size parser_cls)};
###     my $profile = $site->load_profile('user', $arg{'profile'});
### # $batch_size ||= 5;
###     my %parser = %{ $profile->{'parser'} };
###     $parser_cls ||= $parser{'class'} || 'Biblio::FolioX::Util::JSONParser';
###     $parser_cls = 'Biblio::FolioX' . $parser_cls if $parser_cls =~ /^[+]/;
###     delete $parser{'class'};
###     my ($file) = @ARGV;
###     use_class($parser_cls);
###     my $parser = $parser_cls->new('site' => $site, %parser);
###     my $sub = $verb eq 'match' ? \&show_matching_users : \&update_or_create_user;
###     my %count;
###     my $n = 0;
###     $parser->iterate(
###         'file' => $file,
###         'batch_size' => $batch_size,
###         'each' => sub {
###             foreach my $result ($site->match_users($profile, @_)) {
###                 my ($user, $matches) = @$result{qw(user matches)};
###                 my $res = $sub->($site, $file, ++$n, \%arg, $user, @$matches);
###                 $count{$res}++;
###             }
###         },
###     );
###     print "result counts {\n";
###     foreach my $k (sort keys %count) {
###         printf "  %6d %s\n", $count{$k}, $k;
###     }
###     print "}\n";
### }

sub print_user {
}

sub show_matching_users {
    my ($self, %arg) = @_;
    my ($site, $source, $results, $batch_base, $format) = @arg{qw(site source results batch_base format)};
    my $file = $source->{'file'};
    my ($incoming, $matching) = map { $_->{'results'} } @$results{qw(incoming candidates)};
    foreach my $inc (@$incoming) {
        my ($user, $n, $matches) = @$inc{qw(record n matches)};
        $n += $batch_base;
        my $m = @$matches;
        my $res = $m == 1 ? 'one' : $m > 1 ? 'multiple' : 'none';
        if ($format->{'json'}) {
            my $json = $self->json;
            print "# ------------------------------------------------------------------------------\n"
                if $n > 1;
            print $json->encode({
                'index' => $n,
                'input' => $user,
                'matches' => $matches,
                'result' => $res,
            });
        }
        else {
            my $pg = $site->patron_group($user->{'patronGroup'});
            printf "user %d \{\n", $n;
            printf "  hrid:             %s\n", $user->{'hrid'}             if defined $user->{'hrid'};
            printf "  patronGroup:      %s\n", $site->expand_uuid('patronGroup' => $user->{'patronGroup'});
            printf "  username:         %s\n", $user->{'username'}         if defined $user->{'username'};
            printf "  externalSystemId: %s\n", $user->{'externalSystemId'} if defined $user->{'externalSystemId'};
            printf "  lastName:         %s\n", $user->{'personal'}{'lastName'};
            printf "  firstName:        %s\n", $user->{'personal'}{'firstName'};
            printf "  file:             %s\n", $file;
            printf "    row number: %s\n", $n;
            printf "    raw data:   %s\n", $user->{'_raw'};
            printf "    matches:    %d\n", $m;
            my $i = 0;
            foreach my $match (@$matches) {
                $i++;
                my ($matched_user, $matched_by) = @$match{qw(user by)};
                my $bystr = join(', ', @$matched_by);
                printf "  match %d on %s \{\n", $i, $bystr;
                printf "    id:               %s\n", $matched_user->{'id'};
                printf "    hrid:             %s\n", $matched_user->{'hrid'}             if defined $matched_user->{'hrid'};
                printf "    patronGroup:      %s\n", $site->expand_uuid('patronGroup' => $user->{'patronGroup'});
                printf "    username:         %s\n", $matched_user->{'username'}         if defined $matched_user->{'username'};
                printf "    externalSystemId: %s\n", $matched_user->{'externalSystemId'} if defined $matched_user->{'externalSystemId'};
                printf "    lastName:         %s\n", $matched_user->{'personal'}{'lastName'};
                printf "    firstName:        %s\n", $matched_user->{'personal'}{'firstName'};
                print  "  \}\n";
            }
            print "\}\n";
        }
    }
}

sub update_or_create_user {
    my ($self, %arg) = @_;
    my ($site, $source, $result, $batch_base) = @arg{qw(site source result batch_base)};
    my ($user, $matches, $n) = @$result{qw(record matches n)};
    my $file = $source->{'file'};
    $n += $batch_base;
    my $folio = $self->folio;
    if (@$matches == 0) {
        $user->{'id'} ||= $folio->uuid;
        my $res = $site->POST('/users', $user);
        if ($res->is_success) {
            $user = $self->content($res);
            my $id = $user->{'id'};
            print STDERR "ADD user $id created: record $n in $file\n";
        }
        else {
            print STDERR "ERR user not created: record $n in $file: ", $res->status_line, "\n";
        }
    }
    elsif (@$matches == 1) {
        my $match = $matches->[0];
        my $id = $match->{'id'};
        my @changes = $site->update_object('object' => $match, 'using' => $user, 'profile' => $arg{'profile'});
        if (!@changes) {
            print STDERR "IGN user $id unchanged: record $n in $file\n";
        }
        else {
            my $res = $site->PUT("/users/$id", $match);
            if ($res->is_success) {
                print STDERR "UPD user $id updated: record $n in $file\n";
            }
            else {
                print STDERR "ERR user $id not updated: record $n in $file: ", $res->status_line, "\n";
            }
        }
    }
    else {
        print STDERR "UNR user unresolved: record $n in $file\n";
    }
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
    usage if !@$argv;
    my $subcmd = shift @$argv;
    my @caller = caller 1;
    $caller[3] =~ /(cmd_\w+)$/ or die;
    goto &{ $self->can($1.'_'.$subcmd) or usage };
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

sub add_holdings_to_marc_fields {
    my %arg = @_;
    my ($fields, $holdings, $spell_out_locations, $add_items, $classifier) = @arg{qw(fields holdings spell_out_locations add_items classifier)};
    my $num_items = 0;
    foreach my $holding (@$holdings) {
        my $location = $holding->location;
        my $locstr = $spell_out_locations
            ? $location->discoveryDisplayName // $location->name
            : $location->code;
        my $call_number = $holding->call_number;
        undef $call_number if defined $call_number && $call_number !~ /\S/;
        push @$fields, marcfield(
            '852', ' ', ' ',
            'b' => $locstr,
            _optional('h' => $call_number),
# XXX See "crazy hack" above!
            _optional('x' => $classifier),
            '0' => $holding->id,
        );
        if ($add_items) {
            my @items = $holding->items;
            if (@items) {
                $num_items += @items;
                add_items_to_marc_fields(
                    'fields' => $fields,
                    'items' => \@items,
                    'call_number' => $call_number,
                );
            }
        }
    }
    return $num_items;
}

sub add_items_to_marc_fields {
    my %arg = @_;
    my ($fields, $items, $call_number) = @arg{qw(fields items call_number)};
    foreach my $item (@$items) {
        my $iloc = $item->location->code;
        my $vol = $item->volume;
        # my $year = $item->year_caption;
        # my $copies = @{ $item->copy_numbers || [] };
        my $item_call_number = join(' ', grep { defined && length } $call_number, $vol);
        undef $item_call_number if $item_call_number !~ /\S/;
        push @$fields, marcfield(
            '859', ' ', ' ',
            'b' => $iloc,
            defined($item_call_number) ? ('h' => $item_call_number) : (),
            '0' => $item->id,
        );
    }
}

sub _33x_field_subfields {
    my ($fields) = @_;
    my %f33x = map { $_ => {} } qw(336 337 338);
    foreach (@$fields) {
        my $tag = $_->[TAG];
        my $f = $f33x{$tag} or next;
        my $valref = $_->[VALREF];
        next if $$valref !~ /\x1f2rda/;
        if ($$valref =~ /\x1fb([^\x1d-\x1f]+)/) {
            $f33x{$tag}{$1} = 1;
        }
    }
    return \%f33x;
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
    my %sub = ('i' => $arg{'instance_id'}, 's' => $arg{'source_id'});
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
    my @args = (':standard', @_);
    my %arg;
    my %tracing;
    my %optgroup = (
        'standard' => {
            'r|root=s' => \$root,
            's|site=s' => \$site_name,
            'v|verbose' => \$self->{'verbose'},
            'n|dry-run' => \$self->{'dryrun'},
        },
        'fetch' => {
            'm|offset=s' => \$arg{'offset'},
            'z|limit=i' => \$arg{'limit'},
            'o|order-by=s' => \$arg{'order_by'},
        },
        'formats' => {
            'J|as-json' => sub { $arg{'format'}{'json'} = 1 },
            'M|as-marc' => sub { $arg{'format'}{'marc'} = 1 },
            'T|as-text' => sub { $arg{'format'}{'text'} = 1 },
        },
        'debugging' => {
            'X|tracing=s' => sub {
                $self->_parse_tracing(\%tracing, $_[1]);
            },
        },
    );
    my %opt;
    my %not;
    while (@args) {
        my $arg = shift @args;
        my %o = parse_opt_spec($arg);
        my $name = $o{'name'};
        if ($o{'is_group'}) {
            my $opts = $optgroup{$name} || fatal "unrecognized option group: $name";
            push @args, %$opts;
        }
        elsif ($o{'is_negator'}) {
            $not{$name} = 1;
        }
        elsif (@args) {
            my $dest = shift @args;
            if (!ref $dest) {
                my $key = $dest;
                $dest = \$arg{$key};
            }
            $opt{$o{'spec'}} = $dest;
        }
        else {
            usage;
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
    GetOptionsFromArray($argv, %opt) or usage;
    my $folio = Biblio::Folio->new('root' => $root);
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
    $self->json(JSON->new->pretty->canonical->convert_blessed);
    my $cmd = $self->command;
    if ($cmd ne 'login') {
        $self->login_if_necessary($site);
        # print STDERR "Reusing login for $site_name\n"
        #     if !login_if_necessary($site);
    }
    return ($site, %arg) if wantarray;
    usage if grep { defined $arg{$_} } keys %arg;
    return $site;
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
    my $prog = _program_name();
    print STDERR "$prog: @_\n";
    exit 2;
}

sub _program_name {
    (my $prog = $0) =~ s{.*/}{};
    return $prog;
}

sub _program_file {
    return $0;
}

sub usage {
    my $usage;
    my @commands;
    my $cmd;
    foreach my $i (1..3) {
        my @caller = caller($i);
        last if !@caller;
        $cmd = $caller[3];
        last if $cmd =~ s/.+::cmd_//;
        undef $cmd;
    }
    if (@_) {
        $usage = '@SITE ' . shift;
    }
    my $progfile = _program_file();
    if (defined $cmd && open my $fh, '<', $progfile) {
        my $incmd;
        while (<$fh>) {
            $incmd = $1, next if /^sub cmd_(\S+)/;
            undef($incmd), next if /^\}/;
            next if !/^(?:    |\t)(?:my \$)?usage (?:= )? "(.+)"/ || !$incmd;
            push @commands, $1;
            $usage = '@SITE ' . $1 if $incmd eq $cmd;
        }
    }
    $usage ||= '@SITE COMMAND [ARG...]';
    print STDERR "usage: $prog $usage\n";
    print STDERR "commands:\n" if @commands;
    print STDERR '  ', $_, "\n" for sort @commands;
    exit 1;
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
