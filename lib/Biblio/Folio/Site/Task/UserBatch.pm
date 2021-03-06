package Biblio::Folio::Site::Task::UserBatch;

use strict;
use warnings;

use base qw(Biblio::Folio::Site::Task);

use Biblio::Folio::Util qw(
    FORMAT_MARC FORMAT_JSON FORMAT_TEXT
    _json_encode
    _json_begin
    _json_append
    _json_end
    _indentf
    _unbless
);

# Results
use constant qw(OK     0);
use constant qw(FAILED 1);

sub site { @_ > 1 ? $_[0]{'site'} = $_[1] : $_[0]{'site'} }
sub context { @_ > 1 ? $_[0]{'context'} = $_[1] : $_[0]{'context'} }
sub output_file { @_ > 1 ? $_[0]{'output_file'} = $_[1] : $_[0]{'output_file'} }
sub output_fh { @_ > 1 ? $_[0]{'output_fh'} = $_[1] : $_[0]{'output_fh'} }
sub profile { @_ > 1 ? $_[0]{'profile'} = $_[1] : $_[0]{'profile'} }

sub batch { @_ > 1 ? $_[0]{'context'}{'batch'} = $_[1] : $_[0]{'context'}{'batch'} }
sub file { @_ > 1 ? $_[0]{'context'}{'file'} = $_[1] : $_[0]{'context'}{'file'} }
sub record_number { @_ > 1 ? $_[0]{'context'}{'record_number'} = $_[1] : $_[0]{'context'}{'record_number'} }
sub format { @_ > 1 ? $_[0]{'context'}{'format'} = $_[1] : $_[0]{'context'}{'format'} }
sub verb { @_ > 1 ? $_[0]{'context'}{'verb'} = $_[1] : $_[0]{'context'}{'verb'} }

sub all_steps { return qw(pickup validate prepare match load) }

sub kind { 'user' }

sub init {
    my ($self) = @_;
    my $site = $self->{'site'};
    if (!defined $self->{'profile'}) {
        $self->{'profile'} = $site->profile('user');
    }
    elsif (!ref $self->{'profile'}) {
        $self->{'profile'} = $site->profile('user', $self->{'profile'});
    }
    if (defined $self->{'output_file'} && !defined $self->{'output_fh'}) {
        my $f = $self->{'output_file'};
        open my $fh, '>', $f
            or die "open $f: $!";
        $self->{'output_fh'} = $fh;
    }
    else {
        $self->{'output_fh'} ||= \*STDOUT;
    }
    return $self;
}

sub sorter {
    return shift->worker('sorter', 'kind' => 'user_batch', @_);
}

sub parser {
    return shift->worker('parser', @_);
}

sub matcher {
    return shift->worker('matcher', @_);
}

sub loader {
    return shift->worker('loader', @_);
}

sub validator {
    return shift->worker('validator', @_);
}

sub output_container {
    my ($self) = @_;
    my $format = $self->format || $self->format(FORMAT_TEXT);
    if ($format eq FORMAT_JSON) {
        my %context = %{ $self->context };
        my $output = { %context };
        delete @$output{qw(out err)};
        $output->{'profile_name'} = $self->profile->{'name'};
        return $self->{'output'} = $output;
    }
}

sub begin {
    my ($self, $verb, %context) = @_;
    $context{'verb'} = $verb;
    $self->context(\%context);
}

sub end {
    my ($self) = @_;
    my $format = $self->format;
    if ($format eq FORMAT_JSON) {
        my $jctx = delete $self->{'_json_context'};
        _json_end($jctx) if $jctx;
    }
}

sub begin_file {
    my ($self, $file) = @_;
    my $context = $self->context;
    $context->{'file'} = $file;
    $context->{'record_number'} = 0;
}

sub end_file {
    my ($self) = @_;
    my $context = $self->context;
    delete $context->{'file'};
    delete $context->{'record_number'};
}

sub out {
    my ($self, @output) = @_;
    my $file = $self->file;
    my $format = $self->format;
    my $verb = $self->verb;
    foreach (@output) {
        $_->{'file'} = $file;
        $_->{'n'} = $self->{'record_number'}++;
    }
    if ($format eq FORMAT_JSON) {
        my $fhout = $self->output_fh;
        my $jctx = $self->{'_json_context'}
            ||= _json_begin($self->output_container, $verb . '_results', $fhout);
        _json_append($jctx, @output);
        return;
    }
    elsif ($verb eq 'parse') {
        foreach my $user (@output) {
            $self->_show_parsed_user('user' => $user);
        }
        return;
    }
    foreach my $member (@output) {
        my ($n, $ok, $error, $status) = @$member{qw(n ok error status)};
    }
}

# --- Action functions

sub parse {
    my ($self, %arg) = @_;
    my $files = $arg{'files'};
    return if !@$files;
    $self->begin('parse', %arg);
    my $parser = $self->parser(%arg);
    #my (@errors, @parsed, @unparsed);
    my %hooks = (
        'each' => sub {
            my %param = @_;
            my $batch = $param{'batch'};
            $self->out(@$batch);
        },
        'error' => sub {
            my %param = @_;
            my ($file, $n, $err) = @param{qw(file n error)};
            $err = "unknown error" if !defined $err;
            # ($err) = split /\n/, defined $err ? $err : 'unknown error';
            #push @errors, [$file, $n, $err];
            $self->_show_error('prepare', %param, %arg, 'error' => $err);
        },
        #'end' => sub {
        #    my %param = @_;
        #    my $file = $param{'file'};
        #    if (@errors) {
        #        push @unparsed, $file;
        #    }
        #    else {
        #        push @parsed, $file;
        #    }
        #    @errors = ();
        #},
    );
    foreach my $file (@$files) {
        $self->begin_file($file);
        print STDERR "Parsing: $file\n" if $arg{'verbose'};
        $parser->iterate(
            %arg,
            'hooks' => \%hooks,
            'file' => $file,
        );
        $self->end_file;
    }
    $self->end;
}

sub validate {
    my ($self, %arg) = @_;
    my $files = $arg{'files'};
    return if !@$files;
    $arg{'batch_size'} ||= 1;
    my $validator = $self->validator;
    my (@errors, @file_errors);
    my $progress = sub {
        my %param = @_;
        my ($file, $n) = @param{qw(file n)};
        my $nerr = @file_errors;
        printf STDERR "\rValidating... %6d OK %6d errors : %s", $n - $nerr, $nerr, $file;
    };
    my %hooks = (
        'each' => sub {
            my %param = @_;
            $progress->(@_) if $param{'n'} % 20 == 0;
        },
        'error' => sub {
            my %param = @_;
            my ($file, $n, $err) = @param{qw(file n error)};
            push @file_errors, [$file, $n, $err];
            $progress->(@_);
        },
        'end' => sub {
            $progress->(@_);
            print STDERR "\n";
            push @errors, @file_errors;
            @file_errors = ();
        },
    );
    foreach my $file (@$files) {
        $self->begin_file($file);
        $progress->('file' => $file, 'n' => 0);
        $validator->iterate(
            %arg,
            'hooks' => \%hooks,
            'file' => $file,
        );
        $self->end_file;
    }
    my $fhout = $self->output_fh;
    print $fhout join("\t", qw(row file error)), "\n"
        if @errors;
    foreach (@errors) {
        my ($file, $n, $err) = @$_;
        print $fhout join("\t", $n, $file, $err), "\n";
    }
}
### foreach my $f (@invalid) {
###     print STDERR "invalid patron file: $f\n";
### }
### return (OK) if $self->dry_run;
### my $ok;
### eval {
###     ($ok, @valid) =_move_to_dir(@valid, $self->directory('valid'));
###     ($ok, @invalid) = _move_to_dir(@invalid, $self->directory('invalid'));
### };
### my $result = $ok && !@invalid ? OK : FAILED;
### return $result, { @work, 'valid' => \@valid, 'invalid' => \@invalid };

sub match {
    my ($self, %arg) = @_;
    $arg{'batch_size'} ||= 1;
    my $files = $arg{'files'};
    return if !@$files;
    $self->begin('match', %arg);
    my $parser = $self->parser(%arg);
    my $matcher = $self->matcher(%arg);
    my (@errors, @parsed, @unparsed);
    my $verbose = $arg{'verbose'};
    $arg{'format'} ||= FORMAT_TEXT;
    my $batch_base = 1;
    my %hooks = (
        #'before' => sub {
        #    $batch_base = 1;
        #},
        'each' => sub {
            my %param = @_;
            my $batch = $param{'batch'};
            my $results = $matcher->match(@$batch);
            $self->_show_matching_users(%param, %arg, 'batch_base' => $batch_base, 'results' => $results);
            $batch_base += $arg{'batch_size'};
        },
        #'after' => sub {
        #    $batch_base += $arg{'batch_size'};
        #},
    );
    foreach my $file (@$files) {
        $self->begin_file($file);
        $parser->iterate(
            %arg,
            'hooks' => \%hooks,
            'file' => $file,
        );
        $self->end_file;
    }
    $self->end;
}

sub prepare {
    my ($self, %arg) = @_;
    $arg{'batch_size'} ||= 1;
    my $files = $arg{'files'};
    return if !@$files;
    $self->begin('prepare', %arg);
    my $site = $self->site;
    my $parser = $self->parser(%arg);
    my $matcher = $self->matcher(%arg);
    my $loader = $self->loader(%arg);
    my $batch_base = 1;
    my $format = $self->format;
    $arg{'site'} = $site;
    my %hooks = (
        # 'before' => sub { $batch_base = 1 },
        # 'after' => sub { $batch_base += $arg{'batch_size'} },
        'each' => sub {
            my %param = @_;
            my $batch = $param{'batch'};
            my @match_results = $matcher->match(@$batch);
            my $prepared_batch = $loader->prepare(@match_results);
            $self->_show_prepared_users(%param, %arg, 'batch' => $prepared_batch, 'match_results' => \@match_results);
            $batch_base += $arg{'batch_size'};
        },
        'error' => sub {
            my %param = @_;
            my ($n, $err) = @param{qw(n num_errors)};
            $self->_show_error('prepare', %param, %arg, 'error' => $err);
        },
    );
    if (0 && $format eq FORMAT_JSON) {
        my %hash = (
            'batch_size' => $arg{'batch_size'},
            'parser_class' => ref $parser,
            'matcher_class' => ref $matcher,
            'loader_class' => ref $loader,
        );
        my $fhout = $self->output_fh;
        $hooks{'begin'} = sub {
            print $fhout $self->json_begin_hash_array_members(\%hash, 'records');
        };
        $hooks{'end'} = sub {
            print $fhout $self->json_end_hash_array_members;
        };
    }
    foreach my $file (@$files) {
        $self->begin_file($file);
        $parser->iterate(
            %arg,
            'hooks' => \%hooks,
            'file' => $file,
        );
        $self->end_file;
    }
    $self->end;
}

sub load {
    my ($self, %arg) = @_;
    $arg{'batch_size'} ||= 1;
    $self->begin('load', %arg);
    my $files = $arg{'files'};
    my $site = $self->site;
    my $sorter = $self->sorter(%arg);
    $files = [ $sorter->sort(@$files) ];
    $self->end, return if !@$files;
    my $parser = $self->parser(%arg);
    my $matcher = $self->matcher(%arg);
    my $loader = $self->loader(%arg);
    if (0) {
        foreach my $file (@$files) {
            $parser->begin_file($file);
            while (my @users = $parser->next_batch($arg{'batch_size'})) {
                my @match_results = $matcher->match(@users);
                my $batch = $loader->prepare(@match_results);
                # ...
            }
            $parser->end_file;
        }
        return;
    }
    my $batch_base = 1;
    my %hooks = (
        # 'before' => sub { $batch_base = 1 },
        # 'after' => sub { $batch_base += $arg{'batch_size'} },
        'each' => sub {
            my %param = @_;
            my $batch = $param{'batch'};
            my @match_results = $matcher->match(@$batch);
            $batch = $loader->prepare(@match_results);
            #my @objects = $loader->prepare(@match_results);
            if ($self->dry_run) {
                $self->_show_prepared_users(%param, %arg, 'batch' => $batch, 'match_results' => \@match_results);
            }
            else {
                my ($num_ok, $num_failed) = $loader->load($batch);
                $self->_show_user_load_results(%param, %arg, 'batch' => $batch, 'ok' => $num_ok, 'failed' => $num_failed);
            }
            $batch_base += $arg{'batch_size'};
        },
        'error' => sub {
            my %param = @_;
            my $err = $param{'error'} || 'unknown error';
            # XXX What to do?
        },
    );
    $arg{'site'} = $site;
    foreach my $file (@$files) {
        $self->begin_file($file);
        $parser->iterate(
            %arg,
            'hooks' => \%hooks,
            'file' => $file,
        );
        $self->end_file;
    }
    $self->end;
}

# --- Phases

### sub step_pickup {
###     my ($self, @work) = @_;
###     my %arg = (%$self, @work);
###     my $site = $self->site;
###     my $sorter = $self->sorter(%arg); # $site->sorter_for('user_file', %arg);
###     return OK, $sorter->sort;
### }
### 
### sub step_prepare {
###     my ($self, @work) = @_;
###     my %arg = (%$self, @work);
###     my ($site, $files) = @arg{qw(site files)};
###     return (OK) if !@$files;
###     my $parser = $self->parser(%arg);
###     my $matcher = $self->matcher(%arg);
###     my $loader = $self->loader(%arg);
###     my $batch_base = 1;
###     my $format = $self->format;
###     $arg{'site'} = $site;
###     my %hooks = (
###         #'before' => sub {
###         #    $batch_base = 1;
###         #},
###         'each' => sub {
###             my %param = @_;
###             my $batch = $param{'batch'};
###             my @match_results = $matcher->match(@$batch);
###             my $prepared_batch = $loader->prepare(@match_results);
###             $self->_show_prepared_users(%param, %arg, 'batch' => $prepared_batch, 'match_results' => \@match_results);
###             $batch_base += $arg{'batch_size'};
###         },
###         'error' => sub {
###             my %param = @_;
###             my ($n, $err) = @param{qw(n num_errors)};
###             $self->_show_error('prepare', %param, %arg, 'error' => $err);
###         },
###         #'after' => sub {
###         #    $batch_base += $arg{'batch_size'};
###         #},
###     );
###     if ($format eq FORMAT_JSON) {
###         my %hash = (
###             'batch_size' => $arg{'batch_size'},
###             'parser_class' => ref $parser,
###             'matcher_class' => ref $matcher,
###             'loader_class' => ref $loader,
###         );
###         $hooks{'begin'} = sub {
###             print $fhout $self->json_begin_hash_array_members(\%hash, 'records');
###         };
###         $hooks{'end'} = sub {
###             print $fhout $self->json_end_hash_array_members;
###         };
###     }
###     foreach my $file (@$files) {
###         $self->begin_file($file);
###         $parser->iterate(
###             %arg,
###             'hooks' => \%hooks,
###             'file' => $file,
###         );
###         $self->end_file;
###     }
### }
### 
### sub _old_step_prepare {
###     my ($self, @work) = @_;
###     my %arg = (%$self, @work);
###     my ($site, $files) = @arg{qw(site files)};
###     return (OK) if !@$files;
###     my @ready;
###     foreach my $file (@$files) {
###         $self->begin_file($file);
###         my $ok = $self->prepare(
###             'file' => $file,
###             'dry_run' => $self->dry_run,
###             'verbose' => $self->verbose,
###         );
###         if ($ok) {
###             push @ready, $file;
###         }
###         else {
###             print STDERR "prepare failed: $file\n";
###             return FAILED, @ready;
###         }
###         $self->end_file;
###     }
###     my ($ok, @moved) = _move_to_dir(@ready, $self->directory('ready'));
###     return ($ok ? OK : FAILED, @moved);
### }
### 
### sub step_match {
###     my ($self, @work) = @_;
###     my %arg = (%$self, @work);
###     my ($site, $files) = @arg{qw(site files)};
###     return (OK) if !@$files;
###     my $parser = $self->parser(%arg);
###     my $matcher = $self->matcher(%arg);
###     my $batch_base = 1;
###     my %hooks = (
###         #'before' => sub {
###         #    $batch_base = 1;
###         #},
###         'each' => sub {
###             my %param = @_;
###             my $batch = $param{'batch'};
###             my $results = $matcher->match(@$batch);
###             $self->_show_matching_users(%param, %arg, 'batch_base' => $batch_base, 'results' => $results);
###             $batch_base += $arg{'batch_size'};
###         },
###         #'after' => sub {
###         #    $batch_base += $arg{'batch_size'};
###         #},
###     );
###     foreach my $file (@$files) {
###         $self->begin_file($file);
###         $parser->iterate(
###             %arg,
###             'hooks' => \%hooks,
###             'file' => $file,
###         );
###         $self->end_file;
###     }
### }
### 
### sub step_load {
###     my ($self, @work) = @_;
###     my %arg = (%$self, @work);
###     my ($site, $files) = @arg{qw(site files)};
###     return (OK) if !@$files;
###     my $parser = $self->parser(%arg);
###     my $matcher = $self->matcher(%arg);
###     my $loader = $self->loader(%arg);
###     my $batch_base = 1;
###     my %hooks = (
###         #'before' => sub { $batch_base = 1 },
###         #'after' => sub { $batch_base += $arg{'batch_size'} },
###         'each' => sub {
###             my %param = @_;
###             my $batch = $param{'batch'};
###             my @match_results = $matcher->match(@$batch);
###             my $prepared_batch = $loader->prepare(@match_results);
###             #my @objects = $loader->prepare(@match_results);
###             if ($self->dry_run) {
###                 my @members = @{ $prepared_batch->{'members'} };
###                 foreach my $member (@members) {
###                     my ($n, $action, $record, $object, $matches, $warning, $rejected, $matching)
###                         = @$member{qw(n action record object matches warning rejected matching)};
###                     my @matches = $matches ? @$matches : ();
###                     # my $raw = $record->{'_raw'};
###                     my $parsed = $record->{'_parsed'};
###                     $n += $batch_base;
###                     print $fhout "--------------------------------------------------------------------------------\n" if $n;
###                     print $fhout "Record: $n\n";
###                     print $fhout "Action: ", uc $action, "\n";
###                     # print $fhout "Raw input: $raw\n";
###                     print $fhout "Parsed:\n";
###                     print $fhout _indent(_json_encode($parsed));
###                     print $fhout "Matches: ", scalar(@matches), "\n";
###                     my $what = $action eq 'create' ? $record : $object;
###                     if (defined $what) {
###                         print $fhout "Object:\n";
###                         print $fhout _indent(_json_encode($what));
###                         if (@matches > 1 && $action eq 'update' || @matches > 0 && $action eq 'create') {
###                             my $m = 0;
###                             foreach my $match (@matches) {
###                                 $m++;
###                                 # TODO
###                             }
###                         }
###                     }
###                     else {
###                         print $fhout "No match to $action\n";
###                     }
###                 }
###             }
###             else {
###                 my ($ok, $failed) = $loader->load($prepared_batch);
###                 $self->_show_user_load_results(%param, %arg, 'batch' => $prepared_batch, 'ok' => $ok, 'failed' => $failed);
###             }
###             $batch_base += $arg{'batch_size'};
###         },
###     );
###     $arg{'site'} = $site;
###     foreach my $file (@$files) {
###         $self->begin_file($file);
###         $parser->iterate(
###             %arg,
###             'hooks' => \%hooks,
###             'file' => $file,
###         );
###         $self->end_file;
###     }
### }

# --- Supporting functions

sub _show_matching_users {
    my ($self, %arg) = @_;
    my ($site, $source, $results, $batch_base, $format) = @arg{qw(site source results batch_base format)};
    my $file = $source->{'file'};
    my ($incoming, $matching) = map { $_->{'results'} } @$results{qw(incoming candidates)};
    my $fhout = $self->output_fh;
    foreach my $inc (@$incoming) {
        my ($user, $n, $matches) = @$inc{qw(record n matches)};
        $n += $batch_base;
        my $m = @$matches;
        my $res = $m == 1 ? 'one' : $m > 1 ? 'multiple' : 'none';
        if ($format eq FORMAT_JSON) {
            print $fhout "# ------------------------------------------------------------------------------\n"
                if $n > 1;
            print $fhout _json_encode({
                'index' => $n,
                'input' => $user,
                'matches' => $matches,
                'result' => $res,
            });
        }
        else {
            printf $fhout "user %d \{\n", $n;
            print $fhout $self->_user_to_text($user, 2);
            printf $fhout "  file:             %s\n", $file;
            printf $fhout "    row number:     %s\n", $n;
            # printf $fhout "    raw data:       %s\n", $user->{'_raw'};
            printf $fhout "    matches:        %d\n", $m;
            foreach my $i (0..$#$matches) {
                my $match = $matches->[$i];
                my ($matched_user, $matched_by) = @$match{qw(object by)};
                my $bystr = join(', ', @$matched_by);
                printf $fhout "  match %d on %s \{\n", $i, $bystr;
                print $fhout $self->_user_to_text($matched_user, 4);
                print $fhout "  \}\n";
            }
            print $fhout "\}\n";
        }
    }
    1;
}

sub _show_parsed_user {
    my ($self, %arg) = @_;
    my $user = $arg{'user'};
    my $n = $arg{'n'};
    my $fhout = $self->output_fh;
    printf $fhout "user %d \{\n", $n;
    printf $fhout "  file:         %s\n", $arg{'file'};
    printf $fhout "    row number: %s\n", $n;
    # printf $fhout "    raw data:   %s\n", $user->{'_raw'};
    print $fhout $self->_user_to_text($user, 2);
    print $fhout "\}\n";
}

sub _show_unparsed_user {
}

sub _show_prepared_users {
    my ($self, %arg) = @_;
    my $batch = $arg{'batch'};
    my $batch_num = $arg{'batch_num'};
    my $batch_size = $arg{'batch_size'};
    my $batch_base = ($batch_num - 1) * $batch_size + 1;
    my $match_results = $arg{'match_results'};
    my $format = $arg{'format'};
    my $file = $arg{'source'}{'file'};
    my $splitter;
    if ($arg{'split_into'}) {
        my $dest = $arg{'split_into'};
        my $ext = $format eq FORMAT_JSON ? '.json' : '.txt';
        if ($dest =~ /%.+%/) {
        }
        elsif ($dest =~ /%/) {
            $dest =~ s{(?:\Q$ext\E)?$}{-%s$ext};
        }
        else {
            # Split into a directory, use default file name pattern
            $dest =~ s{/?$}{/%03d-%s$ext};
        }
        my $reverse = ($dest =~ /%[0-9]*d.*%s/ ? 1 : 0);
        $splitter = sub {
            my ($name, $n) = @_;
            sprintf $dest, $reverse ? ($n, $name) : ($name, $n);
        };
    }
    my $n = $batch_base;
    my $double_divider = scalar('=' x 80) . "\n";
    my $divider = scalar('-' x 80) . "\n";
    my $fhout = $self->output_fh;
    if ($format eq FORMAT_TEXT) {
        print $fhout $double_divider if !$splitter;
        print $fhout "Batch $batch_num\n";
    }
    foreach my $member ($batch->members) {
        my $action = $member->{'action'};
        my $user = $member->{$action};
        my ($old, $via) = (_unbless($member->{'object'}), _unbless($member->{'record'}));
        my $matches = $member->{'matches'} || [];
        my $m = @$matches;
# TODO: if ($arg{'diff'}) { my $diff = _diff3($old, $via, $new); ... }
        if ($splitter) {
            my %outfile;
            foreach (['old', $old], ['new', $user], ['via', $via], ['changes', $member->{'changes'}]) {
                my ($name, $obj) = @$_;
                next if !$obj;
                my $f = $outfile{$name} = $splitter->($name, $n);
                die "file exists: $f" if -e $f;
                open my $fh, '>', $f or die "open $f for writing: $!";
                if ($format eq FORMAT_JSON) {
                    print $fh _json_encode($obj);
                }
                elsif ($format eq FORMAT_TEXT) {
                    if ($name ne 'changes') {
                        print $fh $self->_user_to_text($obj);
                    }
                    else {
                        my @changes = @{ $obj || [] };
                        if (!@changes) {
                            print $fh "no changes\n";
                        }
                        else {
                            print $fh "Changes:\n";
                            foreach my $change (@changes) {
                                print $fh '  ', join(' ', @$change), "\n";
                            }
                            print $fh "Diff:\n";
                            my ($oldfile, $newfile) = map { $outfile{$_} } qw(old new);
                            print $fh qx/diff -u $oldfile $newfile/;
                        }
                    }
                }
                close $fh;
            }
        }
        else {
            if ($format eq FORMAT_JSON) {
                $self->out({
                    'old' => $old,
                    'new' => $user,
                    'via' => $via,
                    'changes' => $member->{'changes'},
                });
            }
            elsif ($format eq FORMAT_TEXT) {
                my $fhout = $self->output_fh;
                print $fhout $divider;
                printf $fhout "user %d : %s \{\n", $n, $action;
                print $fhout $self->_user_to_text($user, 2);
                printf $fhout "  file:             %s\n", $file;
                printf $fhout "    row number: %s\n", $n;
                # printf $fhout "    raw data:   %s\n", $member->{'record'}{'_raw'};
                printf $fhout "    matches:    %d\n", $m;
                my @changes = @{ $member->{'changes'} || [] };
                if (@changes) {
                    print $fhout "  changes:\n";
                    foreach (@changes) {
                        my ($verb, @etc) = @$_;
                        if ($verb eq 'set') {
                            printf $fhout "    set %s\n",    shift @etc;
                            printf $fhout "      new: %s\n", shift @etc;
                        }
                        elsif ($verb eq 'unset') {
                            printf $fhout "    unset %s\n",  shift @etc;
                            printf $fhout "      old: %s\n", shift @etc;
                        }
                        elsif ($verb eq 'change') {
                            printf $fhout "    change %s\n", shift @etc;
                            printf $fhout "      old: %s\n", shift @etc;
                            printf $fhout "      new: %s\n", shift @etc;
                        }
                        elsif ($verb eq 'keep') {
                            printf $fhout "    keep %s (unchanged)\n", @etc;
                        }
                        elsif ($verb eq 'protected') {
                            printf $fhout "    keep %s (protected)\n", @etc;
                        }
                        elsif ($verb eq 'add') {
                            printf $fhout "    add %s\n", @etc;
                        }
                        else {
                            printf $fhout "    $verb @etc\n";
                        }
                    }
                }
            }
        }
        $n++;
    }
}

sub _show_user_load_results {
    my ($self, %arg) = @_;
    my $batch = $arg{'batch'};
    my $format = $arg{'format'};
    my ($num_ok, $num_failed) = @arg{qw(ok failed)};
    my @members = @{ $batch->{'members'} };
    $self->out(@members);
}

sub _show_error {
    my ($self, $action, %arg) = @_;
    my $batch = $arg{'batch'};
    my $err = $arg{'error'};
    # ($err) = split /\n/, $err;
    1;
}

sub _user_to_text {
    my ($self, $user, $lvl) = @_;
    $lvl ||= 0;
    my $site = $self->site;
    my $personal = $user->{'personal'} || {};
    my $addresses = $personal->{'addresses'} || [];
    my $text = '';
    $text .= _indentf($lvl, "patronGroup:      %s\n", $site->expand_uuid('group', $user->{'patronGroup'}));
    $text .= _indentf($lvl, "id:               %s\n", $user->{'id'})               if defined $user->{'id'};
    $text .= _indentf($lvl, "hrid:             %s\n", $user->{'hrid'})             if defined $user->{'hrid'};
    $text .= _indentf($lvl, "externalSystemId: %s\n", $user->{'externalSystemId'}) if defined $user->{'externalSystemId'};
    $text .= _indentf($lvl, "barcode:          %s\n", $user->{'barcode'})          if defined $user->{'barcode'};
    $text .= _indentf($lvl, "username:         %s\n", $user->{'username'})         if defined $user->{'username'};
    $text .= _indentf($lvl, "lastName:         %s\n", $personal->{'lastName'} || '[none]');
    $text .= _indentf($lvl, "firstName:        %s\n", $personal->{'firstName'} || '[none]');
    $text .= _indentf($lvl, "enrollmentDate:   %s\n", $user->{'enrollmentDate'});
    $text .= _indentf($lvl, "expirationDate:   %s\n", $user->{'expirationDate'});
    $text .= _indentf($lvl, "addresses:        %d\n", scalar @$addresses);
    if (@$addresses) {
        foreach my $i (0..$#$addresses) {
            my $addr = $addresses->[$i];
            my $type = $site->expand_uuid('address_type', $addr->{'addressTypeId'});
            $text .= _indentf($lvl, "address %d {\n", $i);
            $text .= _indentf($lvl+2, "type:           %s\n", $type);
            $text .= _indentf($lvl+2, "primaryAddress: %s\n", $addr->{'primaryAddress'} ? 'true' : 'false');
            foreach my $k (qw(addressLine1 addressLine2 city region postalCode)) {
                my $v = $addr->{$k};
                next if !defined $v || !length $v;
                $text .= _indentf($lvl+2, "%-15s %s\n", $k.':', $v);
            }
            $text .= _indentf($lvl, "}\n");
        }
    }
    return $text;
}

# --- Junk

### sub old_prepare {
###     my $self = shift;
###     my %arg = (%$self, @_);
###     my ($site, $file) = @arg{qw(site file)};
###     my $parser = $self->parser(%arg);
###     my $matcher = $self->matcher(%arg);
###     my $loader = $self->loader(%arg);
###     my $batch_base = 1;
###     my $format = $self->format;
###     $arg{'site'} = $site;
###     if ($format eq FORMAT_JSON) {
###         my %hash = (
###             'batch_size' => $arg{'batch_size'},
###             'parser_class' => ref $parser,
###             'matcher_class' => ref $matcher,
###             'loader_class' => ref $loader,
###         );
###         $arg{'begin'} = sub {
###             print $fhout $self->json_begin_hash_array_members(\%hash, 'records');
###         };
###         $arg{'end'} = sub {
###             print $fhout $self->json_end_hash_array_members;
###         };
###     }
###     $arg{'each'} = sub {
###         my %param = @_;
###         my $batch = $param{'batch'};
###         my @match_results = $matcher->match(@$batch);
###         my $prepared_batch = $loader->prepare(@match_results);
###         $self->_show_prepared_users(%param, %arg, 'batch' => $prepared_batch, 'match_results' => \@match_results);
###     };
###     $arg{'after'} = sub {
###         $batch_base += $arg{'batch_size'};
###     };
###     $parser->iterate(%arg);
### }
###
### sub step_parse {
###     my ($self, @work) = @_;
###     my %arg = (%$self, @work);
###     my ($site, $files) = @arg{qw(site files)};
###     return (OK) if !@$files;
###     my $parser = $self->parser(%arg);
###     my (@errors, @parsed, @unparsed);
###     my %hook = (
###         'each' => sub {
###             my %param = @_;
###             my $batch = $param{'batch'};
###             my ($user) = @$batch;
###             $self->_show_parsed_user(%arg, 'user' => $user);
###         },
###         'error' => sub {
###             my %param = @_;
###             my ($file, $n, $err) = @param{qw(file n error)};
###             ($err) = split /\n/, $err;
###             push @errors, [$file, $n, $err];
###         },
###         'end' => sub {
###             my %param = @_;
###             my $file = $param{'file'};
###             if (@errors) {
###                 push @unparsed, $file;
###             }
###             else {
###                 push @parsed, $file;
###             }
###             @errors = ();
###         },
###     );
###     foreach my $file (@$files) {
###         $self->begin_file($file);
###         $parser->iterate(
###             %arg,
###             %hook,
###             'file' => $file,
###         );
###         $self->end_file;
###     }
### }


1;

