#!/usr/bin/perl

use strict;
use warnings;

use parent qw(Test::Class);
use Test::More;
use Test::Deep;

use lib 'lib';

use IPC::System::Simple;
use autodie qw(system fork);

use Flux::File;
use Flux::In::DiskBuffer;
use Time::HiRes qw(sleep time);
use autodie qw(:all);

sub xqx {
    my $result = qx(@_);
    if ($?) {
        die "xqx(@_) failed: $?, $!";
    }
    return $result;
}

sub setup :Test(setup) {
    system('rm -rf tfiles');
    system('mkdir tfiles');
}

sub linear :Test(4) {
    my $file = Flux::File->new('tfiles/file');
    $file->write("$_\n") for 'a'..'z';
    $file->commit;

    my $in = $file->in('tfiles/cursor');

    my $buffered_in = Flux::In::DiskBuffer->new(in => $in, dir => 'tfiles/buffer');

    is($buffered_in->read, "a\n", 'first item');
    is($buffered_in->read, "b\n", 'second item');
    $buffered_in->commit;

    $buffered_in = Flux::In::DiskBuffer->new(in => $in, dir => 'tfiles/buffer');
    is($buffered_in->read, "c\n", 'read after commit');

    $buffered_in = Flux::In::DiskBuffer->new(in => $in, dir => 'tfiles/buffer');
    is($buffered_in->read, "c\n", 'read after reading without commit');
}

sub read_only :Test(4) {
    my $file = Flux::File->new('tfiles/file');
    $file->write("$_\n") for 'a'..'z';
    $file->commit;

    my $in = $file->in('tfiles/cursor');

    my $buffered_in1 = Flux::In::DiskBuffer->new(in => $in, dir => 'tfiles/buffer', format => 'plain');
    $buffered_in1->read for 1 .. 3;
    # do not commit
    my $buffered_in2 = Flux::In::DiskBuffer->new(in => $in, dir => 'tfiles/buffer', format => 'plain');
    $buffered_in2->read for 1 .. 3;
    undef $buffered_in2;
    # commit neither but unlock
    system("chmod -R -w tfiles/buffer");
    my $buffered_ro = Flux::In::DiskBuffer->new(in => $in, dir => 'tfiles/buffer', read_only => 1, format => 'plain');
    my $data = $buffered_ro->read_chunk(5); # lives_ok?
    is_deeply($data, [map {"$_\n"} ('a'..'e')], "read some");
    is($buffered_ro->lag(), (26-5)*2, "lag from chunks");
    $data = $buffered_ro->read_chunk(10);
    is_deeply($data, [map {"$_\n"} ('f'..'o')], "read more");
    is($buffered_ro->lag(), (26-15)*2, "yet lag");
    system("chmod -R u+w tfiles/buffer");
}

sub gc_bug :Test(4) {
    my $file = Flux::File->new('tfiles/file');
    $file->write("$_\n") for 'a'..'z';
    $file->commit;

    my $in = $file->in('tfiles/cursor');

    my $buffered_in = Flux::In::DiskBuffer->new(in => $in, dir => 'tfiles/buffer');

    is($buffered_in->read, "a\n", 'first item');
    $buffered_in->commit;

    is($buffered_in->read, "b\n", 'second item');
    is($buffered_in->read, "c\n", 'third item');
    $buffered_in->commit;

    $buffered_in->gc;
    is($buffered_in->read, "d\n", "gc didn't break any posfiles");
}


sub in_coderef :Tests {
    my $file = Flux::File->new('tfiles/file');
    $file->write("$_\n") for 'a'..'z';
    $file->commit;

    my $gen_in = sub {
        return Flux::File->new('tfiles/file')->in('tfiles/cursor');
    };

    my $db1 = Flux::In::DiskBuffer->new(in => $gen_in, dir => 'tfiles/buffer');
    my $db2 = Flux::In::DiskBuffer->new(in => $gen_in, dir => 'tfiles/buffer');

    my $db1_done;
    my $db2_done;
    my @db1_data;
    my @db2_data;
    while (not $db1_done or not $db2_done) {
        unless ($db1_done) {
            my $item = $db1->read;
            if ($item) {
                push @db1_data, $item;
            }
            else {
                $db1_done++;
            }
        }
        unless ($db2_done) {
            my $item = $db2->read;
            if ($item) {
                push @db2_data, $item;
            }
            else {
                $db2_done++;
            }
        }
    }
    is(@db1_data + @db2_data, 26, '26 items total');
    ok(@db1_data > 10, 'at least 10 items from first db');
    ok(@db2_data > 10, 'at least 10 items from second db');
}

sub steal_new_chunk_race :Tests {

    my $file = Flux::File->new('tfiles/file');
    my $TOTAL = 50000;
    $file->write("$_\n") for 1 .. $TOTAL;
    $file->commit;

    my $gen_in = sub {
        return Flux::File->new('tfiles/file')->in('tfiles/cursor');
    };

    my $PROCESSES = 5;
    for my $process (1 .. $PROCESSES) {
        fork and next;
        eval {
            my $in = Flux::In::DiskBuffer->new(in => $gen_in, dir => 'tfiles/buffer');
            my $out = Flux::File->new("tfiles/file.$process");
            while () {
                my $line = $in->read();
                last unless $line; # some bugs lead to a premature 'undef' returned from a non ampty DiskBuffer
                $out->write($line);
                if (rand(100) < 3) {
                    $in->commit();
                    undef $in;
                    $in = Flux::In::DiskBuffer->new(in => $gen_in, dir => 'tfiles/buffer');
                }
            }
            $out->commit();
            $in->commit();
        };
        if ($@) {
            warn "$@";
            exit(1);
        }
        exit;
    }
    while () {
        last if wait == -1;
        is($?, 0, "error code");
    }
    my @counts;
    for (1 .. $PROCESSES) {
        push @counts, int(xqx("cat tfiles/file.$_ | wc -l"));
    }
    my $total = int(xqx("cat tfiles/file.* | wc -l"));
    is($total, $TOTAL, "total count");
    for (1 .. $PROCESSES) {
        cmp_ok(shift @counts, ">", $TOTAL/$PROCESSES*0.8, "no premature exit");
    }
}

sub gc_race :Tests {

    my $LINES = $ENV{LINES} || 1000;
    my $WORKERS = $ENV{WORKERS} || 5;

    my $out = Flux::File->new('tfiles/out');

    my $file = Flux::File->new('tfiles/file');
    system('touch tfiles/file');

    for (1 .. $WORKERS) {
        fork and next;
        eval {
            my $time = time;
            my $buffered_in = Flux::In::DiskBuffer->new(
                in => sub { $file->in('tfiles/cursor') },
                dir => 'tfiles/buffer'
            );
            my $i = 0;
            while () {
                $i++;

                $buffered_in->gc if rand() < 5 * $WORKERS / $LINES; # call gc 5 times per worker on average

                $buffered_in->lag if rand() < 5 * $WORKERS / $LINES; # call lag 5 times per worker on average
                #  lag is prone to race contitions too, let's check whether it fails

                last if time >= $time + 3; # should be enough for everyone
                my $line = $buffered_in->read;

                $buffered_in->commit if rand() < 5 * $WORKERS / $LINES;

                # busy-waiting
                # otherwise it's too probable that one worker will block everyone else and then read everything himself
                next unless $line;

                $out->write($line);
            }
            $buffered_in->commit;
            $out->commit;
        };
        if ($@) {
            diag "Worker error: $@";
            exit 1;
        }
        exit 0;
    }

    for (1 .. $LINES) {
        $file->write("$_\n");
        sleep 1 / $LINES;
    }
    $file->commit;

    while () {
        last if wait == -1;
        is($?, 0, "exit code");
    }

    my $files = xqx('find tfiles/buffer | wc -l');
    chomp $files;
    cmp_ok($files, '<', 10, 'not too many files in buffer dir');

    my @lines = sort { $a <=> $b } split /\n/, qx(cat tfiles/out);
    cmp_deeply(\@lines, [1 .. $LINES], "no dups");
}

sub permissions :Tests {
    my $file = Flux::File->new('tfiles/file');
    $file->write("$_\n") for 'a'..'z';
    $file->commit;

    my $in = $file->in('tfiles/cursor');

    my $buffered_in = Flux::In::DiskBuffer->new(in => $in, dir => 'tfiles/buffer');
    $buffered_in->read;

    my @chunks = glob 'tfiles/buffer/*.chunk';
    is scalar @chunks, 1;

    my @stat = stat $chunks[0];
    is sprintf("%o", $stat[2] & 0777), '644';
}


__PACKAGE__->new->runtests;
