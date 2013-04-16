package Flux::In::DiskBuffer::Chunk;

# ABSTRACT: represents one disk buffer chunk

=head1 DESCRIPTION

This module is used internally by L<Flux::In::DiskBuffer>.

It mostly conforms to C<Flux::In> API, except that you have to pre-initialize it manually using C<load> method. It's a C<Flux::File::In> decorator.

All chunks are immutable after they're created.

Creating an instance of this class doesn't create any files. You have to call C<create> first to fill it.

=cut

use Moo;
with 'Flux::In', 'Flux::In::Role::Lag';
use MooX::Types::MooseLike::Base qw(:all);

use autodie qw(rename unlink);

=head1 METHODS

=over

=cut

use Lock::File qw(lockfile);
use Flux::File::Cursor;
use Flux::File;
use Try::Tiny;

has 'dir' => (
    is => 'ro',
    isa => Str,
    required => 1,
);

has 'id' => (
    is => 'ro',
    isa => Int,
    required => 1,
);

has 'read_only' => (
    is => 'ro',
    isa => Bool,
    default => sub { 0 },
);

has '_lock' => (
    is => 'ro',
    lazy => 1,
    default => sub {
        my $self = shift;
        return if $self->read_only;
        return lockfile($self->_prefix.".lock", { blocking => 0 });
    },
);
has '_in' => (
    is => 'rw', # init lazily, once, in load()
);

sub _prefix {
    my $self = shift;
    return $self->dir.'/'.$self->id;
}

=item B<create($data)>

Create the new chunk and fill it with given arrayref of data atomically.

Exception will happen if chunk already exists.

=cut
my $uid = 0;
sub create {
    my $self = shift;
    my ($data) = @_;

    my $file = $self->_prefix.".chunk";

    # we can't use File::Temp here - it creates files with 600 permissions which breaks read-only mode
    my $new_file = $self->_prefix.".tmp.$$.".time.".".($uid++);

    if ($self->_in) {
        die "Can't recreate chunk, $self is already initialized";
    }
    if (-e $file) {
        die "Can't recreate chunk, $file already exists";
    }
    my $storage = Flux::File->new($new_file);
    $storage->write_chunk($data);
    $storage->commit;

    rename $new_file => $file; # autodie takes care of errors
}

=item B<< load($dir, $id) >>

Initialize input stream. You have to call manually it before reading.

=cut
sub load {
    my $self = shift;

    return 1 if $self->_in; # already loaded

    my $prefix = $self->_prefix;
    my $file = "$prefix.chunk";
    return unless -e $file; # this check is unnecessary, but it reduces number of fanthom lock files
    my $lock = $self->_lock;
    return unless $lock or $self->read_only;
    return unless -e $file;

    # it's still possible that file will disappear (if we're in r/o mode and didn't acquire the lock)
    my $storage = Flux::File->new($file);

    my $new = $self->read_only ? "new_ro" : "new";

    my $in;
    try {
        $in = $storage->in("$prefix.status");
    }
    catch {
        if (-e $file) {
            die $_;
        }
        elsif ($lock) {
            die "Internal error: failed to create $file but lock is acquired";
        }
    };
    return unless $in; # probably disappeared

    $self->_in($in);
    return 1; # ok, loaded
}

sub _check_ro {
    my $self = shift;
    die "Stream is read only" if $self->read_only;
}

sub read {
    my $self = shift;
    return $self->_in->read;
}

sub read_chunk {
    my $self = shift;
    return $self->_in->read_chunk(@_);
}

sub commit {
    my $self = shift;
    $self->_check_ro();
    return $self->_in->commit;
}

sub lag {
    my $self = shift;
    unless ($self->load) {
        return 0; # probably locked in some other process, or maybe chunk is already deleted
    }
    return $self->_in->lag;
}

=item B<< cleanup() >>

Remove all chunk-related files if and only if the chunk is fully processed.

=cut
sub cleanup {
    my $self = shift;
    my $prefix = $self->_prefix;
    return if -e "$prefix.chunk";
    $self->_lock or return; # even if chunk doesn't exist, we'll force the lock
    $self->remove;
}

=item B<< remove() >>

Remove chunk and all related files.

=cut
sub remove {
    my $self = shift;
    $self->_check_ro();

    my $prefix = $self->_prefix;
    for (map { "$prefix.$_" } qw/ chunk status status.lock lock /) {
        unlink $_ if -e $_;
    }
}

=back

=cut

1;
