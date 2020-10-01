#!/usr/bin/env perl

use Modern::Perl '2015';
use bytes;

use DBI;
use Digest::SHA ();
use File::Temp qw(mkstemp);
use FindBin qw($RealBin);
use List::Util qw(shuffle);
use UUID::FFI;

use Test2::V0;
use Test2::Tools::Basic;
use Test2::Tools::Compare;
use Test2::Tools::Subtest;
use Test2::Plugin::NoWarnings;

# Use a weird number of rows so we (most likely) won't match any Bucardo batch sizes
my $row_count = 1681;

my @dsn = map { "dbi:Pg:dbname=lo_test_$_" } (0..4);

plan($row_count);

chdir $RealBin or die;
my $workdir = 'tmp';
mkdir $workdir;
chdir $workdir or die;

my @dbh;
my %db_attr = ( AutoCommit => 0, RaiseError => 1 );

connect_dbs();

# Build a buffer of random bytes to draw from since generating new random bytes
# for every lo is too slow, and we don't need true randomness
my $random_buffer_len = 8192;
my $random_buffer = join('', map { chr(int(rand(256))) } (1..$random_buffer_len));
my $bufsize = 512;
my $max_loc = $random_buffer_len - $bufsize;

my $max_normal_file_size = 1024;
my $max_file_size = 300 * 1024 * 1024;

sub get_num_format { '%' . length('' . $_[0]) . 'd' }

my $file_size_format = get_num_format($max_file_size);
my $file_size_padding = ' ' x length('' . $max_file_size);
my $row_count_format = get_num_format($row_count);

my $sha_type = 256;

my %files;

sub make_lo {
    my $db = shift;

    # Skew distribution of file sizes
    my $bloat = rand();
    my $size = int(do {
        if    ($bloat < 0.87)  { 0 }
        elsif ($bloat < 0.95)  { rand($max_normal_file_size) }
        elsif ($bloat < 0.999) { rand($max_normal_file_size * 1000) }
        else                   { rand($max_file_size) }
    });

    my ($fh, $filename) = create_temp_file();
    my $size_left = $size;
    while ($size_left > 0) {
        my $send_size = ($size_left < $bufsize) ? $size_left : $bufsize;
        $size_left -= $send_size;
        my $random_loc = int(rand($max_loc));
        print $fh substr($random_buffer, $random_loc, $send_size);
    }

    seek($fh, 0, 0);
    my $sha = Digest::SHA->new($sha_type);
    $sha->addfile($fh);
    my $digest = $sha->digest;

    close $fh or die;

    my $dbh = $dbh[$db];
    my $oid = $dbh->pg_lo_import($filename);

    my $file_info = $files{$filename} = {
        digest => $digest,
        oid    => $oid,
        size   => $size,
    };

    my $size_pretty = sprintf($file_size_format, $size);
    diag "$size_pretty random bytes put in file $filename & lo created in db $db";

    return ($filename => $file_info);
}

# Weight the tables so we spend less time on the multi-lo case
my @tables = qw(
    lo_store
    lo_store
    lo_store
    lo_store
    lo_store
    lo_store_multi
);

=for later

    lo_store_manual
    lo_store_manual
    lo_store_manual

=cut

my %nullable_table_column = (
    lo_store_multi => { loid1 => undef },
);

my %sth_cache;
my @rows;
for (1..$row_count) {
    # Randomly choose one of the tables to insert lobs to
    my $table = $tables[int(rand() * @tables)];
    my $nullable_check = $nullable_table_column{$table};

    # TODO: do some UPDATEs too
    my $statement_type = 'INSERT';

    my ($file_count, $columns, $placeholders, $oid_columns) = table_to_files_columns($table, 'INSERT');
    my $sql = "INSERT INTO $table (" . join(', ', @$columns) . ") VALUES ($placeholders)";

    my $db = ($table eq 'lo_store_manual') ? 0 : int(rand() * @dbh);

    my $dbh = $dbh[$db];
    my $sth = $sth_cache{$db}{$table}{$statement_type} ||= $dbh->prepare($sql);
    my (@filenames, @oids);
    for (my $i = 0; $i < $file_count; $i++) {
        my $oid_column = $oid_columns->[$i];
        my ($filename, $oid);

        # Leave a small number of NULLable lo fields empty to exercise that
        my $nullable = ($nullable_check and exists $nullable_check->{$oid_column});
        if ($nullable and rand() < 0.2) {
            $filename = get_random_name();
            diag $file_size_padding . " Tying NULL lo to file id $filename";
        }
        else {
            my $file_info;
            ($filename, $file_info) = make_lo($db, $nullable);
            $oid = $file_info->{oid};
        }

        push @filenames, $filename;
        push @oids, $oid;
    }
    $sth->execute(@filenames, $db, @oids);

    push @rows, {
        db        => $db,
        table     => $table,
        filenames => \@filenames,
    };

    my $row_pretty = sprintf($row_count_format, scalar(@rows));
    diag "Row $row_pretty/$row_count $statement_type to db $db table $table";
}

close_database();


# The waiting time needed will of course vary per Bucardo setup.
my $sleep = 2 + int($row_count / 20);
diag "Sleeping $sleep seconds for replication to complete";
sleep $sleep;


# TODO: Implement this verification part in another language? Could export %files as JSON and read that in to use.

connect_dbs();

for my $row_cache (shuffle @rows) {
    my $filenames = $row_cache->{filenames};
    my $table = $row_cache->{table};
    my $test_name = "Table $table row " . join(', ', @$filenames);
    subtest_buffered $test_name => sub {
        plan(scalar @dsn);

        # Verify that the same large object(s) made it to all databases
        for (my $db = 0; $db < @dbh; $db++) {
            my $dbh = $dbh[$db];
            subtest_buffered "Database $db" => sub {
                plan(2 + @$filenames);

                my ($file_count, $columns, $placeholders, $oid_columns) = table_to_files_columns($table, 'SELECT');
                my $sql = "SELECT * FROM $table WHERE (" . join(', ', @$columns) . ") = ($placeholders)";
                my $sth = $sth_cache{$db}{$table}{SELECT} ||= $dbh->prepare($sql);
                $sth->execute(@$filenames);
                my $row = $sth->fetchrow_hashref;
                SKIP: {
                    my $found = defined($row->{originating_db});
                    ok($found, "have a row");
                    $found or skip("missing row, so skipping remaining tests", 2);

                    is($row->{originating_db}, $row_cache->{db}, "originating_db matches");

                    my ($fh, $new_filename) = create_temp_file();

                    for (my $i = 0; $i < @$columns; $i++) {
                        my $oid_col = $oid_columns->[$i];
                        my $oid = $row->{$oid_col};

                        my $file_col = $columns->[$i];
                        my $filename = $row->{$file_col};

                        subtest_buffered "Column $oid_col, $file_col=$filename" => sub {
                            my $file = $files{$filename};
                            if ($file) {
                                plan(3);

                                my $success = $dbh->pg_lo_export($oid, $new_filename);
                                ok($success, "pg_lo_export");

                                # Seek to flush buffers to disk so file metadata is current
                                seek($fh, 0, 0);
                                is(-s $new_filename, $file->{size}, "size matches");

                                my $sha = Digest::SHA->new($sha_type);
                                $sha->addfile($fh);
                                my $digest = $sha->digest;
                                is($digest, $file->{digest}, "digest matches");
                            }
                            else {
                                plan(1);
                                ok(!defined($oid), "NULL lo oid");
                            }
                        };
                    }
                }
            };
        }
    };
}

close_database();


# File::Temp name randomness is very weak and collides when rapidly called,
# so use our own mkstemp equivalent
sub create_temp_file {
    my $filename = get_random_name();
    my $fh = IO::File->new($filename, "w+");
    $fh->binmode;
    return ($fh, $filename);
}

sub get_random_name { UUID::FFI->new_random->as_hex }

sub table_to_files_columns {
    my ($table, $statement) = @_;
    my ($file_count, @columns, @oid_columns);
    if ($table eq 'lo_store_multi') {
        $file_count = 3;
        @columns  = qw( id1 id2 id3 );
        @oid_columns = qw( loid1 loid2 loid3 );
    }
    else {
        $file_count = 1;
        @columns  = qw( id );
        @oid_columns = qw( loid );
    }
    push @columns, 'originating_db', @oid_columns if $statement eq 'INSERT';
    my $placeholders = join ',', ('?') x @columns;
    my @return = ($file_count, \@columns, $placeholders, \@oid_columns);
    return @return;
}

sub connect_dbs {
    @dbh = map {
        DBI->connect($_, undef, undef, \%db_attr) or die
    } @dsn;
}

sub close_database {
    for my $db (keys %sth_cache) {
        my $d = $sth_cache{$db};
        for my $table (keys %$d) {
            my $t = $sth_cache{$db}{$table};
            for my $statement (keys %$t) {
                local $_ = $sth_cache{$db}{$table}{$statement};
                $_ and $_->finish;
            }
        }
    }

    $_ and $_->commit, $_->disconnect for @dbh;
}
