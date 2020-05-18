package Biblio::Folio::Site::Harvest;

# my $harvest = Biblio::Folio::Site::Harvest->new(
#     'kinds' => [qw(item holdings_record instance)],
#     'searchers' => {
#         'item' => ['@query' => $cql, '@limit' => 1000],
#         'holdings_record' => ['@query' => $cql, '@limit' => 1000],
#         'instance' => ['@query' => $cql, '@limit' => 1000],
#     },
# );

sub new {
    my $cls = shift;
    my $self = bless { @_ }, $cls;
    $self->init;
    return $self;
}

sub init {
    my ($self) = @_;
    my %kind = %{ $self->{'kinds'} };
    my $objects   = $self->{'objects'}   ||= map { $_ => {} } keys %kind;
    my $queues    = $self->{'queues'}    ||= map { $_ => [] } keys %kind;
    my $searchers = $self->{'searchers'} ||= map { $_ => [] } keys %kind;
    my $seers     = $self->{'seers'}     ||= map { $_ => [] } keys %kind;
    while (my ($k, $kind) = each %kind) {
        my $p = $kind{'parent'};
        if ($p) {
            my $pk = $p->{'kind'};
            my $pf = $kind{'parent_id_field'};
        }
    }
    @$self{qw(objects queues searchers seers)} = (\%object, \%queue, \%searchers, \%seer);
}

