#!/usr/bin/env perl

use Modern::Perl '2015';
use bytes;

use DBI;
use Digest::SHA ();
use File::Temp qw(mkstemp);
use FindBin qw($RealBin);
use UUID::FFI;

use Test2::V0;
use Test2::Tools::Basic;
use Test2::Tools::Compare;
use Test2::Tools::Subtest;
use Test2::Plugin::NoWarnings;

# Use a weird number of files so we (most likely) won't match any Bucardo batch sizes
my $file_count = 777;

my @dsn = map { "dbi:Pg:dbname=lo_test_$_" } (0..4);

plan($file_count);

chdir $RealBin or die;
my $workdir = 'tmp';
mkdir $workdir;
chdir $workdir or die;

my @dbh;
my %db_attr = ( AutoCommit => 0, RaiseError => 1 );

sub connect_dbs {
    @dbh = map {
        DBI->connect($_, undef, undef, \%db_attr) or die
    } @dsn;
}

connect_dbs();

# File::Temp name randomness is very weak and collides when rapidly called,
# so use our own mkstemp equivalent
sub create_temp_file {
    my $filename = UUID::FFI->new_random->as_hex;
    my $fh = IO::File->new($filename, "w+");
    $fh->binmode;
    return ($fh, $filename);
}

# Build a buffer of random bytes to draw from since generating new random bytes
# for every lo is too slow, and we don't need true randomness
my $random_buffer_len = 8192;
my $random_buffer = join('', map { chr(int(rand(256))) } (1..$random_buffer_len));
my $bufsize = 512;
my $max_loc = $random_buffer_len - $bufsize;

my $max_normal_file_size = 102_400;
my $max_file_size = $max_normal_file_size * 100;

sub get_num_format { '%' . length('' . $_[0]) . 'd' }

my $file_size_format = get_num_format($max_file_size);
my $file_count_format = get_num_format($file_count);

my $sha_type = 256;

my %files;
my @sth_insert;

for (my $i = 0; $i < $file_count; $i++) {
    my ($fh, $filename) = create_temp_file();

    my $size = int(rand($max_normal_file_size));
    # Make a little subset of the files much larger
    my $bloat = rand();
    if    ($bloat > 0.95) { $size *= 100; }
    elsif ($bloat > 0.9)  { $size *=  10; }

    # Choose a random database to write this object to
    my $index = int(rand() * @dbh);

    my $lo_pretty = sprintf($file_count_format, $i + 1);
    my $size_pretty = sprintf($file_size_format, $size);
    diag "lo $lo_pretty/$file_count: $size_pretty random bytes in file $filename imported to db $index";

    my $size_left = $size;
    while ($size_left > 0) {
        my $send_size = ($size_left < $bufsize) ? $size_left : $bufsize;
        $size_left -= $send_size;
        my $random_loc = int(rand($max_loc));
        my $chunk = substr($random_buffer, $random_loc, $send_size);
        print $fh substr($random_buffer, $random_loc, $send_size);
    }

    seek($fh, 0, 0);
    my $sha = Digest::SHA->new($sha_type);
    $sha->addfile($fh);
    my $digest = $sha->digest;

    close $fh or die;

    my $dbh = $dbh[$index];
    my $loid = $dbh->pg_lo_import($filename);

    $files{$filename} = {
        digest          => $digest,
        oid             => $loid,
        size            => $size,
        originating_db  => $index,
    };

    # TODO: do some UPDATEs too
    # TODO: test table with more than 1 lo column
    my $sth = $sth_insert[$index] ||= $dbh->prepare("INSERT INTO lo_store (id, originating_db, loid) VALUES (?,?,?)");
    $sth->execute($filename, $index, $loid);

=for skip

    # optionally commit every once in a while so Bucardo can go replicate what we have so far
    next if $i % 150 != 0;
    diag "Committing transactions";
    $_->commit for @dbh;

=cut

}

$_ and $_->finish for @sth_insert;
$_ and $_->commit, $_->disconnect for @dbh;


my $sleep = 45;
diag "Sleeping $sleep seconds for replication to complete";
sleep $sleep;


# TODO: Implement this verification part in another language? Could export %files as JSON and read that in to use.

connect_dbs();

my @sth_select;

for my $filename (keys %files) {
    my $file = $files{$filename};
    subtest_buffered "File $filename" => sub {
        plan(scalar @dsn);

        # Verify that the same large object made it to all databases
        for (my $index = 0; $index < @dbh; $index++) {
            my $dbh = $dbh[$index];
            subtest_buffered "Database $index" => sub {
                plan(5);

                my $sth = $sth_select[$index] ||= $dbh->prepare("SELECT originating_db, loid FROM lo_store WHERE id = ?");
                $sth->execute($filename);
                my ($originating_db, $loid) = $sth->fetchrow_array;
                SKIP: {
                    ok($loid, "have an loid");
                    $loid or skip("missing loid, so skipping remaining tests", 4);

                    is($originating_db, $file->{originating_db}, "originating_db matches");

                    my ($fh, $new_filename) = create_temp_file();

                    my $success = $dbh->pg_lo_export($loid, $new_filename);
                    ok($success, "pg_lo_export");

                    # Seek to flush buffers to disk so file metadata is current
                    seek($fh, 0, 0);
                    is(-s $new_filename, $file->{size}, "size matches");

                    my $sha = Digest::SHA->new($sha_type);
                    $sha->addfile($fh);
                    my $digest = $sha->digest;
                    is($digest, $file->{digest}, "digest matches");
                }
            };
        }
    };
}

$_ and $_->finish for @sth_select;
$_ and $_->commit, $_->disconnect for @dbh;
