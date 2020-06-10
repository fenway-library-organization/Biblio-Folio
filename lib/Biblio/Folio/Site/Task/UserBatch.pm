package Biblio::Folio::Site::Task::UserBatch;

use strict;
use warnings;

use base qw(Biblio::Folio::Site::Task);

use Biblio::Folio::Util qw(
    FORMAT_MARC FORMAT_JSON FORMAT_TEXT
    _indentf
);

# Results
use constant qw(OK     0);
use constant qw(FAILED 1);

sub site { @_ > 1 ? $_[0]{'site'} = $_[1] : $_[0]{'site'} }
sub batch { @_ > 1 ? $_[0]{'batch'} = $_[1] : $_[0]{'batch'} }
sub format { @_ > 1 ? $_[0]{'format'} = $_[1] : $_[0]{'format'} }
sub profile { @_ > 1 ? $_[0]{'profile'} = $_[1] : $_[0]{'profile'} }

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

# --- Phases

sub step_pickup {
    my ($self, @work) = @_;
    my %arg = (%$self, @work);
    my $site = $self->site;
    my $sorter = $self->sorter(%arg); # $site->sorter_for('user_file', %arg);
    return OK, $sorter->sort;
}

sub step_validate {
    my ($self, @work) = @_;
    my %arg = (%$self, @work);
    my ($site, $files) = @arg{qw(site files)};
    return (OK) if !@$files;
    my $validator = $self->validator;
    my (@errors, @valid, @invalid);
    my %hooks = (
        'error' => sub {
            my ($n, $err) = @_;
            ($err) = split /\n/, $err;
            push @errors, [$n, $err];
        },
        'last' => sub {
            my %param = @_;
            my $file = $param{'file'};
            if (@errors) {
                push @invalid, $file;
            }
            else {
                push @valid, $file;
            }
            @errors = ();
        },
    );
    foreach my $file (@$files) {
        $validator->iterate(
            %arg,
            %hooks,
            'file' => $file,
        );
    }
    foreach my $f (@invalid) {
        print STDERR "invalid patron file: $f\n";
    }
    return (OK) if $self->dry_run;
    my $ok;
    eval {
        ($ok, @valid) =_move_to_dir(@valid, $self->directory('valid'));
        ($ok, @invalid) = _move_to_dir(@invalid, $self->directory('invalid'));
    };
    my $result = $ok && !@invalid ? OK : FAILED;
    return $result, { @work, 'valid' => \@valid, 'invalid' => \@invalid };
}

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
###             $self->_print_user(%arg, 'user' => $user);
###         },
###         'error' => sub {
###             my %param = @_;
###             my ($file, $n, $err) = @param{qw(file n error)};
###             ($err) = split /\n/, $err;
###             push @errors, [$file, $n, $err];
###         },
###         'last' => sub {
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
###         $parser->iterate(
###             %arg,
###             %hook,
###             'file' => $file,
###         );
###     }
### }

sub step_prepare {
    my ($self, @work) = @_;
    my %arg = (%$self, @work);
    my ($site, $files) = @arg{qw(site files)};
    return (OK) if !@$files;
    my $parser = $self->parser(%arg);
    my $matcher = $self->matcher(%arg);
    my $loader = $self->loader(%arg);
    my $batch_base;
    my $format = $self->format;
    $arg{'site'} = $site;
    $arg{'before'} = sub {
        $batch_base = 1;
    };
    if ($format eq FORMAT_JSON) {
        my %hash = (
            'batch_size' => $arg{'batch_size'},
            'parser_class' => ref $parser,
            'matcher_class' => ref $matcher,
            'loader_class' => ref $loader,
        );
        $arg{'begin'} = sub {
            print $self->json_begin_hash_array_members(\%hash, 'records');
        };
        $arg{'end'} = sub {
            print $self->json_end_hash_array_members;
        };
    }
    $arg{'each'} = sub {
        my %param = @_;
        my $batch = $param{'batch'};
        my @match_results = $matcher->match(@$batch);
        my $prepared_batch = $loader->prepare(@match_results);
        $self->_show_prepared_users(%param, %arg, 'batch' => $prepared_batch, 'match_results' => \@match_results);
    };
    $arg{'after'} = sub {
        $batch_base += $arg{'batch_size'};
    };
    foreach my $file (@$files) {
        $parser->iterate(
            %arg,
            'file' => $file,
        );
    }
}

sub _old_step_prepare {
    my ($self, @work) = @_;
    my %arg = (%$self, @work);
    my ($site, $files) = @arg{qw(site files)};
    return (OK) if !@$files;
    my @ready;
    foreach my $file (@$files) {
        my $ok = $self->prepare(
            'file' => $file,
            'dry_run' => $self->dry_run,
            'verbose' => $self->verbose,
        );
        if ($ok) {
            push @ready, $file;
        }
        else {
            print STDERR "prepare failed: $file\n";
            return FAILED, @ready;
        }
    }
    my ($ok, @moved) = _move_to_dir(@ready, $self->directory('ready'));
    return ($ok ? OK : FAILED, @moved);
}

sub step_match {
    my ($self, @work) = @_;
    my %arg = (%$self, @work);
    my ($site, $files) = @arg{qw(site files)};
    return (OK) if !@$files;
    my $parser = $self->parser(%arg);
    my $matcher = $self->matcher(%arg);
    my $batch_base;
    $arg{'before'} = sub {
        $batch_base = 1;
    };
    $arg{'each'} = sub {
        my %param = @_;
        my $batch = $param{'batch'};
        my $results = $matcher->match(@$batch);
        $self->_show_matching_users(%param, %arg, 'batch_base' => $batch_base, 'results' => $results);
    };
    $arg{'after'} = sub {
        $batch_base += $arg{'batch_size'};
    };
    foreach my $file (@$files) {
        $parser->iterate(%arg);
    }
}

sub step_load {
    my ($self, @work) = @_;
    my %arg = (%$self, @work);
    my ($site, $files) = @arg{qw(site files)};
    return (OK) if !@$files;
    my $parser = $self->parser(%arg);
    my $matcher = $self->matcher(%arg);
    my $loader = $self->loader(%arg);
    my $batch_base;
    $arg{'before'} = sub { $batch_base = 1 };
    $arg{'after'} = sub { $batch_base += $arg{'batch_size'} };
    $arg{'each'} = sub {
        my %param = @_;
        my $batch = $param{'batch'};
        my @match_results = $matcher->match(@$batch);
        my $prepared_batch = $loader->prepare(@match_results);
        #my @objects = $loader->prepare(@match_results);
        if ($self->dry_run) {
            my @members = @{ $prepared_batch->{'members'} };
            foreach my $member (@members) {
                my ($n, $action, $record, $object, $matches, $warning, $rejected, $matching)
                    = @$member{qw(n action record object matches warning rejected matching)};
                my @matches = $matches ? @$matches : ();
                # my $raw = $record->{'_raw'};
                my $parsed = $record->{'_parsed'};
                $n += $batch_base;
                print "--------------------------------------------------------------------------------\n" if $n;
                print "Record: $n\n";
                print "Action: ", uc $action, "\n";
                # print "Raw input: $raw\n";
                print "Parsed:\n";
                print _indent(_json_encode($parsed));
                print "Matches: ", scalar(@matches), "\n";
                my $what = $action eq 'create' ? $record : $object;
                if (defined $what) {
                    print "Object:\n";
                    print _indent(_json_encode($what));
                    if (@matches > 1 && $action eq 'update' || @matches > 0 && $action eq 'create') {
                        my $m = 0;
                        foreach my $match (@matches) {
                            $m++;
                            # TODO
                        }
                    }
                }
                else {
                    print "No match to $action\n";
                }
            }
        }
        else {
            my ($ok, $failed) = $loader->load($prepared_batch);
            $self->show_user_load_results(%param, %arg, 'batch' => $prepared_batch, 'ok' => $ok, 'failed' => $failed);
        }
    };
    $arg{'site'} = $site;
    foreach my $file (@$files) {
        $parser->iterate(
            %arg,
        );
    }
}

# --- Action functions

sub parse {
    my ($self, %arg) = @_;
    my $files = $arg{'files'};
    return if !@$files;
    my $parser = $self->parser(%arg);
    my (@errors, @parsed, @unparsed);
    my %hook = (
        'each' => sub {
            my %param = @_;
            my $batch = $param{'batch'};
            my ($user) = @$batch;
            $self->_print_user(%arg, %param, 'user' => $user);
        },
        'error' => sub {
            my %param = @_;
            my ($file, $n, $err) = @param{qw(file n error)};
            ($err) = split /\n/, defined $err ? $err : 'unknown error';
            push @errors, [$file, $n, $err];
        },
        'last' => sub {
            my %param = @_;
            my $file = $param{'file'};
            if (@errors) {
                push @unparsed, $file;
            }
            else {
                push @parsed, $file;
            }
            @errors = ();
        },
    );
    foreach my $file (@$files) {
        $parser->iterate(
            %arg,
            %hook,
            'file' => $file,
        );
        1;
    }
    1;
}

sub match {
    my $self = shift;
}

sub prepare {
    my $self = shift;
}

sub load {
    my $self = shift;
}

# --- Supporting functions

sub _show_matching_users {
    my ($self, %arg) = @_;
    my ($site, $source, $results, $batch_base, $format) = @arg{qw(site source results batch_base format)};
    my $file = $source->{'file'};
    my ($incoming, $matching) = map { $_->{'results'} } @$results{qw(incoming candidates)};
    foreach my $inc (@$incoming) {
        my ($user, $n, $matches) = @$inc{qw(record n matches)};
        $n += $batch_base;
        my $m = @$matches;
        my $res = $m == 1 ? 'one' : $m > 1 ? 'multiple' : 'none';
        if ($format eq FORMAT_JSON) {
            print "# ------------------------------------------------------------------------------\n"
                if $n > 1;
            print _json_encode({
                'index' => $n,
                'input' => $user,
                'matches' => $matches,
                'result' => $res,
            });
        }
        else {
            printf "user %d \{\n", $n;
            print $self->_user_to_text($user, 2);
            printf "  file:             %s\n", $file;
            printf "    row number:     %s\n", $n;
            # printf "    raw data:       %s\n", $user->{'_raw'};
            printf "    matches:        %d\n", $m;
            foreach my $i (0..$#$matches) {
                my $match = $matches->[$i];
                my ($matched_user, $matched_by) = @$match{qw(object by)};
                my $bystr = join(', ', @$matched_by);
                printf "  match %d on %s \{\n", $i, $bystr;
                print $self->_user_to_text($matched_user, 4);
                print "  \}\n";
            }
            print "\}\n";
        }
    }
    1;
}

sub _print_user {
    my ($self, %arg) = @_;
    my $user = $arg{'user'};
    my $n = $arg{'n'};
    printf "user %d \{\n", $n;
    printf "  file:         %s\n", $arg{'file'};
    printf "    row number: %s\n", $n;
    # printf "    raw data:   %s\n", $user->{'_raw'};
    print $self->_user_to_text($user, 2);
    print "\}\n";
}

sub _show_prepared_users {
    my ($self, %arg) = @_;
    my $batch = $arg{'batch'};
    my $batch_size = $arg{'batch_size'};
    my $batch_base = $arg{'batch_base'} || 1;
    my $batch_number = ($batch_base - 1) / $batch_size;
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
    print $double_divider if !$splitter;
    print "Batch $batch_number\n";
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
                print _json_encode({
                    'old' => $old,
                    'new' => $user,
                    'via' => $via,
                    'changes' => $member->{'changes'},
                });
            }
            elsif ($format eq FORMAT_TEXT) {
                print $divider;
                printf "user %d : %s \{\n", $n, $action;
                print $self->_user_to_text($user, 2);
                printf "  file:             %s\n", $file;
                printf "    row number: %s\n", $n;
                # printf "    raw data:   %s\n", $member->{'record'}{'_raw'};
                printf "    matches:    %d\n", $m;
                my @changes = @{ $member->{'changes'} || [] };
                if (@changes) {
                    print "  changes:\n";
                    foreach (@changes) {
                        my ($verb, @etc) = @$_;
                        if ($verb eq 'set') {
                            printf "    set %s to %s\n", @etc;
                        }
                        elsif ($verb eq 'unset') {
                            printf "    unset %s (was %s)\n", @etc;
                        }
                        elsif ($verb eq 'change') {
                            printf "    change %s from %s to %s\n", @etc;
                        }
                        elsif ($verb eq 'keep' || $verb eq 'add') {
                            printf "    %s %s\n", $verb, @etc;
                        }
                        elsif ($verb eq 'protected') {
                            printf "    keep %s (protected)\n", @etc;
                        }
                        else {
                            printf "    $verb @etc\n";
                        }
                    }
                }
            }
        }
        $n++;
    }
}

sub show_user_load_results {
    my ($self, %arg) = @_;
    my $batch = $arg{'batch'};
    my ($num_ok, $num_failed) = @arg{qw(ok failed)};
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

sub old_prepare {
    my $self = shift;
    my %arg = (%$self, @_);
    my ($site, $file) = @arg{qw(site file)};
    my $parser = $self->parser(%arg);
    my $matcher = $self->matcher(%arg);
    my $loader = $self->loader(%arg);
    my $batch_base = 1;
    my $format = $self->format;
    $arg{'site'} = $site;
    if ($format eq FORMAT_JSON) {
        my %hash = (
            'batch_size' => $arg{'batch_size'},
            'parser_class' => ref $parser,
            'matcher_class' => ref $matcher,
            'loader_class' => ref $loader,
        );
        $arg{'begin'} = sub {
            print $self->json_begin_hash_array_members(\%hash, 'records');
        };
        $arg{'end'} = sub {
            print $self->json_end_hash_array_members;
        };
    }
    $arg{'each'} = sub {
        my %param = @_;
        my $batch = $param{'batch'};
        my @match_results = $matcher->match(@$batch);
        my $prepared_batch = $loader->prepare(@match_results);
        $self->_show_prepared_users(%param, %arg, 'batch' => $prepared_batch, 'match_results' => \@match_results);
    };
    $arg{'after'} = sub {
        $batch_base += $arg{'batch_size'};
    };
    $parser->iterate(%arg);
}

1;
