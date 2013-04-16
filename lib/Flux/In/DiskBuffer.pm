package Flux::In::DiskBuffer;

# ABSTRACT: parallelize any input stream using on-disk buffer

=head1 SYNOPSIS

    $buffered_in = Flux::In::DiskBuffer->new({ in => $in, dir => $dir });

=head1 DESCRIPTION

Some input streams don't support parallel reading from multiple processes simultaneously.

L<Flux::In::DiskBuffer> solves this problem by caching small backlog in multiple on-disk files, allowing them to be processed in parallel.

=head1 ATTRIBUTES

I<in> and I<dir> are required. Everything else is optional.

=over

=cut

use Moo;
with 'Flux::In';

use MooX::Types::MooseLike::Base qw(:all);

use Carp;
use autodie qw(mkdir);
use Scalar::Util qw(blessed);
use Log::Any qw($log);

use Hash::Persistent;
use Lock::File qw(lockfile);

use Flux::In::DiskBuffer::Chunk;

=item B<in>

C<$in> can be a L<Flux::In> object or coderef which returns L<Flux::In> object.

Second variant is useful if underlying input stream caches stream position; in this case, it's necessary to recreate an input stream every time when diskbuffer caches new portion of data, so that each item is read from underlying stream only once. For example, you definitely should wrap C<Flux::Log::In> into C<sub { ... }>.

=cut
has 'in_cb' => (
    is => 'ro',
    isa => CodeRef,
    coerce => sub {
        my $in = $_[0];
        return blessed($in) ? sub { $in } : $in;
    },
    init_arg => 'in',
    required => 1,
);

=item B<in>

Path to a local dir. It will be created automatically if at its parent dir exists (you should have appropriate rights to do this, of course).

=cut
has 'dir' => (
    is => 'ro',
    isa => Str,
    required => 1,
);

=item B<gc_period>

Period in seconds to run garbage collecting.

=back

=cut
has 'gc_period' => (
    is => 'ro',
    isa => Int,
    default => 300,
);

=item B<read_only>

Enable read-only mode. No locks will be taken, commit and gc are forbidden. Useful if DiskBuffer data belongs to a different user, and you either don't want to get an exception, or don't want to corrupt other user's data.

=cut
has 'read_only' => (
    is => 'ro',
    isa => Bool,
);

=item I<read_lock>

Take a short read lock while reading data from the underlying input stream.

This option is true by default. You can turn it off if you're sure that underlying input stream is locking itself. (It should lock on read and then unlock on commit to cooperate correctly with DiskBuffer.)

=cut
has 'read_lock' => (
    is => 'ro',
    isa => Bool,
    default => 1,
);

has '_prev_chunks' => (
    is => 'rw',
    default => sub { {} },
);

has '_uncommited' => (
    is => 'rw',
    default => sub { 0 },
);

sub _check_ro {
    my $self = shift;
    local $Carp::CarpLevel = 1;
    croak "Stream is read only" if $self->read_only;
}

sub BUILD {
    my $self = shift;

    unless (-d $self->dir) {
        $self->_check_ro;
        mkdir $self->dir;
    }

    unless ($self->read_only) {
        $self->try_gc;
    }
}

=item B<< in() >>

Get underlying input stream.

=cut
sub in {
    my $self = shift;
    return $self->in_cb->();
}

sub metadata {
    my $self = shift;
    $self->_check_ro; # read_only meta is not currently of any use
    return Hash::Persistent->new($self->dir."/meta");
}

sub _next_id {
    my $self = shift;
    $self->_check_ro();
    my $status = $self->metadata;
    $status->{id} ||= 0;
    $status->{id}++;
    $status->commit;
    return $status->{id};
}

sub _chunk2id {
    my $self = shift;
    my ($file) = @_;

    my ($id) = $file =~ /(\d+)\.chunk$/ or die "Wrong file $file";
    return $id;
}

sub _chunk {
    my $self = shift;
    my ($id, $overrides) = @_;
    my $chunk = Flux::In::DiskBuffer::Chunk->new(
        dir => $self->dir,
        id => $id,
        read_only => $self->read_only,
        ($overrides ? %$overrides : ()),
    );
}

sub _new_chunk {
    my $self = shift;
    $self->_check_ro();
    my $chunk_size = $self->_uncommited + 1;

    my $read_lock;
    $read_lock = lockfile($self->dir."/read_lock") if $self->read_lock;

    my $in = $self->in;

    my $data = $in->read_chunk($chunk_size);
    return unless $data;
    return unless @$data;

    my $new_id = $self->_next_id;
    my $chunk = $self->_chunk($new_id);
    $chunk->create($data);
    $in->commit;

    return $new_id;
}

sub _load_chunk {
    my ($self, $id, $overrides) = @_;
    $overrides ||= {};

    # this line can create lock file for already removed chunk, which will be removed only at next gc()
    # TODO - think how we can fix it
    my $chunk = $self->_chunk($id, $overrides);
    $chunk->load or return;
    return $chunk;
}

sub _next_chunk {
    my $self = shift;

    my $try_chunk = sub {
        my $chunk_name = shift;
        my $id = $self->_chunk2id($chunk_name);
        if ($self->_prev_chunks->{$id}) {
            return; # chunk already processed in this process
        }

        my $chunk = $self->_load_chunk($id) or return;

        $log->debug("Reading $chunk_name");
        $self->{chunk} = $chunk;
        return 1;
    };

    my @chunk_files = glob $self->dir."/*.chunk";

    @chunk_files =
        map { $_->[1] }
        sort { $a->[0] <=> $b->[0] }
        map { [ $self->_chunk2id($_), $_ ] }
        @chunk_files; # resort by ids

    for my $chunk (@chunk_files) {
        $try_chunk->($chunk) and return 1;
    }

    # no chunks left
    unless ($self->read_only) {
        while () { # the newly create chunk may be stolen by a concurrent process
            my $new_chunk_id = $self->_new_chunk or return;
            $try_chunk->($self->dir."/$new_chunk_id.chunk") and return 1;
        }
    }
    return;
}

=item B<< read() >>

Read new item from queue.

This method chooses next unlocked chunk and reads it from position saved from last invocation in persistent file C<$queue_dir/clients/$client_name/$chunk_id.status>.

=cut
sub read_chunk {
    my ($self, $data_size) = @_;

    my $remaining = $data_size;
    my $result;
    while () {
        unless ($self->{chunk}) {
            unless ($self->_next_chunk()) {
                if ($self->read_only and not $self->{chunk_in}) {
                    $self->{chunk} = $self->in; # ah yeah! streams... yummy!
                    $self->{chunk_in} = 1;
                } else {
                    last;
                }
            }
        }
        my $data = $self->{chunk}->read_chunk($remaining);
        unless (defined $data) {
            # chunk is over
            unless ($self->{chunk_in}) {
                $self->_prev_chunks->{ $self->{chunk}->id } = $self->{chunk};
                delete $self->{chunk};
                next;
            } else {
                last;
            }
        }
        $self->_uncommited($self->_uncommited + scalar @$data);
        $result ||= [];
        push @$result, @$data;
        $remaining -= @$data;
        last if $remaining <= 0;
    }
    return $result;
}

sub read {
    my $self = shift;
    my $chunk = $self->read_chunk(1);
    return unless $chunk;
    return $chunk->[0];
}


=item B<< commit() >>

Save positions from all read chunks; cleanup queue.

=cut
sub commit {
    my $self = shift;
    $self->_check_ro();
    $log->debug("Commiting ".$self->dir);

    if ($self->{chunk}) {
        $self->{chunk}->commit;
        delete $self->{chunk};
    }

    $_->remove for values %{ $self->_prev_chunks };
    $self->_prev_chunks({});
    $self->_uncommited(0);

    return;
}

=item B<< try_gc() >>

Do garbage collecting if it's necessary.

=cut
sub try_gc {
    my $self = shift;
    if ($self->read_only or $self->{gc_timestamp_cached} and time < $self->{gc_timestamp_cached} + $self->{gc_period}) {
        return;
    }

    my $meta = $self->metadata;
    unless (defined $meta->{gc_timestamp}) {
        $self->{gc_timestamp_cached} = $meta->{gc_timestamp} = time;
        $meta->commit;
        return;
    }
    $self->{gc_timestamp_cached} = $meta->{gc_timestamp};
    if (time > $meta->{gc_timestamp} + $self->{gc_period}) {
        $self->{gc_timestamp_cached} = $meta->{gc_timestamp} = time;
        $meta->commit;
        $self->gc($meta);
    }
}

=item B<< gc() >>

Cleanup lost files.

=cut
sub gc {
    my ($self, $meta) = @_;
    $self->_check_ro();
    $meta ||= $self->metadata;

    opendir my $dh, $self->dir or die "Can't open '".$self->dir."': $!";
    while (my $file = readdir $dh) {
        next if $file eq '.';
        next if $file eq '..';
        next if $file =~ /^meta/;
        next if $file =~ /^read_lock$/;
        next if $file =~ /^\d+\.chunk$/;
        if (my ($id) = $file =~ /^(\d+)\.(?: lock | status | status\.lock )$/x) {
            my $chunk = $self->_chunk($id);
            $chunk->cleanup;
            next;
        }

        my $abs_file = $self->dir."/$file";

        if ($file =~ /^\d+\.tmp\./) {
            my $ctime = (stat $abs_file)[10];
            next unless $ctime; # tmp file already disappeared
            my $age = time - $ctime;
            if (-e $abs_file and $age > 600) { # tmp file is too old
                unlink $abs_file;
                $log->debug("Temp file $file removed");
            }
            next;
        }

        $log->warn("Removing unknown file $file");
        unlink $abs_file;
    }
    closedir $dh or die "Can't close '".$self->dir."': $!";
}

=item B<< buffer_lag() >>

Get lag of this buffer (this is slightly less than total buffer size, so this method is called B<buffer_lag()> instead of B<buffer_size()> for a reason).

=cut
sub buffer_lag {

    my $self = shift;
    my $lag = 0;

    my @chunk_files = glob $self->dir."/*.chunk";
    for (@chunk_files) {
        my $id = $self->_chunk2id($_);
        next if $self->_prev_chunks->{$id};
        if ($self->{chunk} and not $self->{chunk_in} and $self->{chunk}->id eq $id) {
            $lag += $self->{chunk}->lag();
        } else {
            my $chunk = $self->_load_chunk($id, { read_only => 1 }) or next; # always read_only
            $lag += $chunk->lag();
        }
    }
    return $lag;
}

=item B<< lag() >>

Get the total lag of this buffer and underlying stream.

=cut
sub lag {
    my $self = shift;
    my $in = $self->in;
    die "underlying input stream doesn't implement Lag role" unless $in->does('Flux::In::Role::Lag');
    return $in->lag + $self->buffer_lag;
}

around 'does' => sub {
    my $orig = shift;
    my $self = shift;
    return 1 if $orig->($self, @_);

    my ($role) = @_;

    if ($role eq 'Flux::In::Role::Lag') {
        return $self->in->does($role);
    }
    return;
};

1;
