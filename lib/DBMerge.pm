package DBMerge;

use 5.014;
use Moose;
use autodie;
use DBI               qw();
use Algorithm::Diff   qw();
use List::Util        qw(min max);
use JSON              qw();
use Data::Dumper      qw();


has verbose         => ( is => 'ro', default => 0 );
has dump_width      => ( is => 'ro', default => 80 );
has source_dsn      => ( is => 'ro', required => 1 );
has target_dsn      => ( is => 'ro', required => 1 );
has source_db_user  => ( is => 'ro' );
has target_db_user  => ( is => 'ro' );

has source_db_attr  => (
    is      => 'ro',
    lazy    => 1,
    default => sub { {}; },
);

has target_db_attr  => (
    is      => 'ro',
    lazy    => 1,
    default => sub { {}; },
);

has source_dbh => (
    is      => 'ro',
    lazy    => 1,
    default => sub {
        my $self = shift;
        $self->_db_connect(
            $self->source_dsn, $self->source_db_user, $self->source_db_attr
        );
    },
);

has target_dbh => (
    is      => 'ro',
    lazy    => 1,
    default => sub {
        my $self = shift;
        $self->_db_connect(
            $self->target_dsn, $self->target_db_user, $self->target_db_attr
        );
    },
);

has unconverted => (
    is      => 'ro',
    lazy    => 1,
    builder => '_build_unconverted',
);

has 'errors' => (
    traits  => ['Counter'],
    is      => 'ro',
    isa     => 'Num',
    default => 0,
    handles => {
        inc_errors   => 'inc',
    },
);

has mappings => (
    is      => 'ro',
    lazy    => 1,
    default => sub { return {} },
);

has source_table_columns => (
    is      => 'ro',
    lazy    => 1,
    default => sub { return $_[0]->_list_table_columns($_[0]->source_dbh); },
);

has target_table_columns => (
    is      => 'ro',
    lazy    => 1,
    default => sub { return $_[0]->_list_table_columns($_[0]->target_dbh); },
);


sub do_merge {
    my $class = shift;

    my $self = $class->new(@_);
    $self->handle_each_table;
    $self->check_unconverted;
    return $self->check_errors;
}


sub DEMOLISH {
    my($self) = @_;
    $self->source_dbh->disconnect if $self->source_dbh;
    $self->target_dbh->disconnect if $self->target_dbh;
}


sub handle_each_table {
    my($self) = @_;

    my $table_handlers = $self->table_handlers;
    my $unconverted = $self->unconverted;
    for(my $i = 0; $i < $#{$table_handlers}; $i += 2) {
        my $table   = $table_handlers->[$i];
        my $args    = $table_handlers->[$i + 1] or next;
        my $handler = shift @$args;
        my $method  = 'handle_' . $handler;
        if($self->can($method)) {
            if($table and $handler ne 'skip') {
                $self->check_columns($table);
            }
            $self->$method($table, @$args);
            delete $unconverted->{$table};
        }
        elsif("$handler" =~ /^\d+$/) {
            next;
        }
        else {
            die "don't know how to handle: '$handler'";
        }
    }
}


sub table_handlers {
    my($self) = @_;

    say ref($self) . " class does not define the table_handlers() method!\n\n"
      . "The table_handlers() method should return an arrayref describing how to\n"
      . "handle each table.  The following might be a useful starting point:\n";

    my($len1, $len2) = (0, 0);
    my $sql = q{
        SELECT
            c.relname AS table,
            c.reltuples AS row_count
        FROM pg_class c
        LEFT OUTER JOIN pg_namespace ns ON (ns.oid = c.relnamespace)
        WHERE ns.nspname = 'public'
        AND c.relkind = 'r'
        ORDER BY 1;
    };
    my @tables = $self->map_source_rows($sql, sub {
        my($row) = @_;
        my $table = $row->{table};
        return if $table =~ /^_mapping_/;
        $len1 = length($table) if length($table) > $len1;
        my $row_count = _underscorify($row->{row_count});
        $len2 = length($row_count) if length($row_count) > $len2;
        return [ $table, $row_count ];
    });

    say "\$table_handlers = [     # row_count_estimates";
    foreach my $tab (@tables) {
        printf("    %*s => [ %*s ],\n", -$len1, $tab->[0], $len2, $tab->[1]);
    }
    say "];";
    say "\nsub table_handlers { return \$table_handlers; }";
    exit 1;
}


sub _underscorify {
   local $_  = shift;
   1 while s/^([-+]?\d+)(\d{3})/$1_$2/;
   return $_;
}


sub handle_skip_empty {
    my($self, $table) = @_;
    my $count = $self->source_row_count("$table");
    if($count > 0) {
        $self->error("Expected table '$table' to be empty, found $count row(s)");
    }
    else {
        say "$table - empty, skipped";
    }
}


sub handle_skip_subset {
    my($self, $table) = @_;
    say "$table - checking all source records already present in target";
    my @cols          = $self->source_columns_for_table($table);
    my $col_list      = join ', ', @cols;
    my $all_col_nums  = join ', ', 1 .. scalar(@cols);
    my $sql = qq{
        SELECT $col_list FROM $table ORDER BY $all_col_nums
    };

    my $row_to_str = sub {
        my($row) = @_;
        return join '|', map { defined $_ ? $_ : '<NULL>' } @{$row}{@cols};
    };
    my @source_rows = $self->map_source_rows($sql, $row_to_str);
    my @target_rows = $self->map_target_rows($sql, $row_to_str);

    my $diff = Algorithm::Diff->new( \@source_rows, \@target_rows );
    my $subset_ok = 1;
    my @report;
    while( $diff->Next() ) {
        my $hunk_diff = $diff->Diff() or next;
        if($diff->Items(1)) {
            $subset_ok = 0;
            $self->error("Can't find row for: $_") foreach $diff->Items(1);
        }
        push @report, "-$_" foreach $diff->Items(1);
        push @report, "+$_" foreach $diff->Items(2);
    }
    if(not $subset_ok) {
        say "Full diff for table '$table'";
        say '#', join '|', @cols;
        say foreach @report;
    }
}


sub handle_skip {
    my($self, $table) = @_;
    say "$table - not needed, skipped";
}


sub handle_check_no_errors {
    my($self) = @_;
    if(my $count = $self->errors) {
        die "Unable to proceed with migrating data following above error(s)\n";
    }
warn "Mappings defined so far: " . Data::Dumper::Dumper($self->mappings);
}


sub handle_build_mapping {
    my($self, $table, $unique_col, $pk_col) = @_;

    say "$table - building mapping using $unique_col as unique key";

    my %target_id;
    my $table_pk = "$table.$pk_col";
    my $sql = qq{
        SELECT $unique_col AS value, $pk_col AS pk
        FROM $table
        WHERE $unique_col IS NOT NULL
        AND $unique_col <> ''
    };
    $self->each_target_row($sql, sub {
        my($row) = @_;
        my $value = $row->{value} // die "NULL value in target $table.$unique_col";
        my $pk    = $row->{pk}    // die "NULL pk in target $table_pk";
        if(exists $target_id{$value}) {
            my $curr = $target_id{$value};
            $curr = [ $curr ] unless ref($curr);
            push @$curr, $pk;
            $target_id{$value} = $curr;
        }
        $target_id{$value} = $pk;
    });

    my %map;
    $self->each_source_row($sql, sub {
        my($row) = @_;
        my $value   = $row->{value} // die "NULL value in source $table.$unique_col";
        my $src_pk  = $row->{pk}    // die "NULL pk in source $table_pk";
        if(exists $map{$src_pk}) {
            die "Duplicate rows for source $table.$unique_col = $value";
        }
        if(my $trg_pk = $target_id{$value}) {
            if(ref($trg_pk)) {
                die "Duplicate rows for target $table.$unique_col = $value";
            }
            $map{$src_pk} = $trg_pk;
        }
        else {
            $self->error("No mapping for $table.$unique_col = $value");
        }
    });
    my $mappings = $self->mappings;
    if($mappings->{$table_pk}) {
        die "Mapping for $unique_col was already set up before processing $table";
    }
    $mappings->{$table_pk} = \%map;
}


sub handle_mapping_table {
    my($self, $table, $unique_col, $pk_col) = @_;

    say "$table - loading mapping from table: _mapping_$table";

    my $table_pk = "$table.$pk_col";
    my(%map, %check);
    my $sql = qq{
        SELECT
            t.$pk_col             AS source_pk,
            m.source_$unique_col  AS source_unique,
            m.source_$pk_col      AS source_map_pk,
            m.target_$pk_col      AS target_map_pk
        FROM $table t
        LEFT OUTER JOIN _mapping_$table m ON (
            t.$pk_col = m.source_$pk_col
        )
    };
    $self->each_source_row($sql, sub {
        my($row) = @_;
        my $source_id = $row->{source_map_pk};
        if(defined($source_id)) {
            my $target_id = $row->{target_map_pk};
            $map{$source_id} = $target_id;
            $check{$target_id} = $row->{source_unique} if defined $target_id;
        }
        else {
            $self->error("No mapping for $table.$pk_col = $row->{source_pk}");
        }
    });

    $sql = qq{
        SELECT $pk_col as target_pk FROM $table;
    };
    $self->each_target_row($sql, sub {
        my($row) = @_;
        my $target_id = $row->{target_pk};
        delete $check{$target_id};
    });
    foreach my $target_pk (sort keys %check) {
        my $unique_val = $check{$target_pk};
        $self->error(
            "No target $table.$pk_col = $target_pk (expected '$unique_val')"
        );
    }

    my $mappings = $self->mappings;
    if($mappings->{$table_pk}) {
        die "Mapping for $unique_col was already set up before processing $table";
    }
    $mappings->{$table_pk} = \%map;
}


sub handle_copy_rows {
    my($self, $table, $spec) = @_;

    say "$table - copying records to target DB";
    $spec = $self->parse_copy_spec($table, $spec);
    $self->each_source_row(
        "SELECT * FROM $table $spec->{condition} ORDER BY $spec->{order_by}",
        $spec->{copy_sub}
    );
    my $skipped = $spec->{rows_skipped} ? " ($spec->{rows_skipped} skipped)" : '';
    my $s = $spec->{rows_copied} == 1 ? '' : 's';
    say "  Copied $spec->{rows_copied} '$table' record$s$skipped";
    if(my $filename = $spec->{save_mapping}) {
        my $mappings = $self->mappings;
        my $map_name = $spec->{mapping_name};
        my $this_map = $mappings->{$map_name} or die "No mapping '$map_name'";
        open my $fh, '>', $filename;
        print $fh JSON->new->pretty->encode($this_map);
        say "  Saved mapping file: $filename";
    }
    $spec = undef;    # Break circular refs
}


sub parse_copy_spec {
    my($self, $table, $raw) = @_;

    my $spec = {
        table         => $table,
        rows_copied   => 0,
        rows_skipped  => 0,
    };
    if(my $column_mappings = delete $raw->{column_mappings}) {
        $spec->{mapper} = $self->build_mapper($column_mappings);
    }
    $spec->{pre_map_method}   = delete $raw->{pre_map_method};
    $spec->{post_map_method}  = delete $raw->{post_map_method};
    $spec->{condition}        = delete $raw->{condition} // '';
    $spec->{order_by}         = delete $raw->{order_by}
        or die "No 'order_by' for table '$table'";
    $spec->{omit_columns}     = delete $raw->{omit_columns} || [];
    if($spec->{mapping_col}   = delete $raw->{make_mapping}) {
        $spec->{mapping_name} = "$table.$spec->{mapping_col}";
    }
    if($spec->{save_mapping}  = delete $raw->{save_mapping}) {
        die "'save_mapping' requires 'make_mapping'"
            unless $spec->{mapping_name};
    }
    if(my $liid = delete $raw->{last_insert_id}) {
        $spec->{liid_seq} = $liid->[0];
        $spec->{liid_col} = $liid->[1];
    }
    if(my @unknown = sort keys %$raw) {
        my $s = @unknown == 1 ? '' : 's';
        die "Unknown key$s in copy_rows: " . join(', ', @unknown) . "\n";
    }
    $spec->{copy_sub} = $self->make_copy_sub($spec);
    return $spec;
}


sub make_copy_sub {
    my($self, $spec) = @_;

    my $table = $spec->{table};

    my($map_col, $output_map);
    if(my $mapping_name = $spec->{mapping_name}) {
        $map_col = $spec->{mapping_col};
        my $mappings = $self->mappings;
        die "There is already a mapping called '$mapping_name'"
            if $mappings->{$mapping_name};
        $output_map = $mappings->{$mapping_name} = {};    # TODO method
    }

    $spec->{total_rows} =
        $self->_db_row_count($self->source_dbh, "$table $spec->{condition}");
    $spec->{insert_sub} = $self->prepare_copy_insert($spec);
    my $row_num = 0;

    return sub {
        my($old) = @_;
        $row_num++;
        print "$row_num/$spec->{total_rows}\r" if $row_num % 100 == 0;
        my $new = { %$old };
        eval {
            if(my $method = $spec->{pre_map_method}) {
                if(($self->$method($old) // '') eq 'SKIP') {
                    $spec->{rows_skipped}++;
                    $self->skip_record;  # Throws exception
                }
            }
            if($spec->{mapper}) {
                $spec->{mapper}->($self, $old, $new);
            }
            $self->dump_record($table, $old, $new) if $self->verbose;
            if(my $method = $spec->{post_map_method}) {
                if(($self->$method($new) // '') eq 'SKIP') {
                    $spec->{rows_skipped}++;
                    $self->skip_record;  # Throws exception
                }
            }
            $spec->{insert_sub}->($new);
            1;
        } or do {
            my $mesg = "$@";
            if($self->was_skipped($@)) {
                say "[--Skipped--]" if $self->verbose;
            }
            else {
                die $mesg . $self->dump_record($table, $old, $new)
            }
        };
        if($map_col) {
            $output_map->{ $old->{$map_col} } = $new->{$map_col};
        }
        $spec->{rows_copied}++;
    };
}


sub skip_record {
    die "SKIP\n";
}


sub was_skipped {
    my $self = shift;
    my $exception = shift // '';
    return !ref($exception) && $exception eq "SKIP\n";
}


sub build_mapper {
    my($self, $mappings) = @_;

    return unless $mappings;

    my @work;
    while(@$mappings) {
        my $map_col  = shift @$mappings;
        my $map_spec = shift @$mappings;
        if(my($method, $args) = $map_spec =~ m{(\w+)\((.*)\)}) {
            push @work, $self->make_method_mapper($map_col, $method, $args);
        }
        else {
            push @work, $self->make_simple_mapper($map_col, $map_spec);
        }
    }
    return sub {
        my($app, $old, $new) = @_;
        $_->($app, $old, $new) foreach @work;
    };
}


sub make_method_mapper {
    my($self, $map_col, $method, $arg_string) = @_;

    my @arg_keys;
    $arg_string =~ s/\s+//g;
    foreach my $arg (split /,/, $arg_string) {
        if(my($prefix, $name) = $arg =~ m{^(old|new)[.]([*\w]+)$}) {
            push @arg_keys, [ $prefix, $name ];
        }
        else {
            push @arg_keys, [ 'new', $arg ];
        }
    }

    return sub {
        my $app = shift;
        my $data = { old => shift, new => shift };
        my @args;
        foreach my $a (@arg_keys) {
            my($prefix, $name) = @$a;
            my $v = $data->{$prefix} // die "invalid prefix: $prefix";
            if($name && $name ne '*') {
                die "No column '$name'" unless exists $v->{$name};
                $v = $v->{$name};
            }
            push @args, $v;
        }
        $data->{new}->{$map_col} = $app->$method(@args);
    }
}


sub make_simple_mapper {
    my($self, $map_col, $map_name) = @_;

    my $mapping = $self->mappings->{$map_name}
        or die "Invalid mapping name: '$map_name'";

    return sub {
        my($app, $old, $new) = @_;
        my $old_val = $new->{$map_col};
        return if not defined $old_val;  # don't map NULLs
        die "Mapping '$map_name' has no entry for old value = $old_val"
            unless exists $mapping->{$old_val};
        $new->{$map_col} = $mapping->{$old_val}
            // die "Mapping from '$map_name' old value = $old_val is NULL but needed";
    }
}


sub prepare_copy_insert {
    my($self, $spec) = @_;

    my $omit_spec = $spec->{omit_columns};
    $omit_spec = [ $omit_spec ] unless ref $omit_spec;
    my %omit = map { $_ => 1 } @$omit_spec;
    my @col_names = grep {
        not $omit{$_}
    } $self->source_columns_for_table($spec->{table});
    my $col_list = join ', ', @col_names;
    my $placeholders = join ', ', ('?') x scalar(@col_names);
    my $sql = qq{
        INSERT INTO $spec->{table} (
            $col_list
        ) VALUES (
            $placeholders
        )
    };
    my $liid_col  = $spec->{liid_col};
    my $liid_seq  = $spec->{liid_seq};
    my $liid_hint = $liid_seq ? { sequence => $liid_seq } : undef;
    my $dbh = $self->target_dbh;
    my $sth = $dbh->prepare($sql);
    return sub {
        my($new) = @_;
        my @params = map { $new->{$_} } @col_names;
        $sth->execute(@params);
        if($liid_seq) {
            $new->{$liid_col} = $dbh->last_insert_id(
                undef, undef, $spec->{table}, undef, $liid_hint
            );
        }
    };
}


sub dump_record {
    my($self, $table, $old, $new) = @_;
    my $two_col = ref($new) ? 1 : 0;

    my @cols = $self->source_columns_for_table($table);
    my $len_c = max(6, map { length } @cols);
    my $max_v = $two_col
              ? int(($self->dump_width - $len_c - 6) / 2)
              : $self->dump_width - $len_c - 3;
    $max_v = max($max_v, 10);
    my $len_v = min(
        $max_v,
        max(
            map {
                defined($_) ? length($_) : 6
            } values %$old, $new ? values %$new : ()
        )
    );
    my $null_str = '<NULL>';
    my($rec1, $rec2) = map {
        my %rec = %$_;
        while(my($key, $val) = each %rec) {
            if(not defined($val)) {
                $val = $null_str;
            }
            elsif($val eq $null_str) {
                $val = "'$null_str'";
            }
            else {
                $val =~ s{\\}{\\\\}g;
                $val =~ s{\r?\n}{\\n}g;
            }
            if(length($val) > $len_v) {
                $val = substr($val, 0, $len_v - 4) . ' ...';
            }
            $rec{$key} = $val;
        }
        \%rec;
    } grep { $_ } $old, $new;
    my $out = '=' x ($len_c + 3 + $len_v + ($two_col ? $len_v + 3 : 0));
    $out .= "\n";
    foreach my $name (@cols) {
        if($two_col) {
            my($val1, $val2) = map { $_->{$name} } $rec1, $rec2;
            my $sep = $val1 eq $val2 ? ' | ' : ' > ';
            $out .= sprintf(
                "%*s | %*s%s%s\n", -$len_c, $name, -$len_v, $val1, $sep, $val2
            );
        }
        else {
            $out .= sprintf("%*s | %s\n", -$len_c, $name, $rec1->{$name});
        }
    }

    print $out unless defined wantarray;
    return $out;
}


sub check_columns {
    my($self, $table) = @_;

    my $source_cols = join '|', $self->source_columns_for_table($table);
    my $target_cols = join '|', $self->target_columns_for_table($table);
    return if $source_cols eq $target_cols;
    die "Columns for table '$table' differ!\n"
      . "source: $source_cols\n"
      . "target: $target_cols\n";
}


sub check_unconverted {
    my($self) = @_;
    my $tables = $self->unconverted;
    foreach my $table (sort keys %$tables) {
        next if $table =~ /^_mapping_/;
        $self->error("No conversion specified for table: $table");
    }
}


sub check_errors {
    my($self) = @_;
    my $count = $self->errors;
    if($count) {
        say "Total error count: " . $self->errors;
        return 1;
    }
    $self->target_commit;
    say "DB merge completed without errors!";
    return 0;
}


sub error {
    my($self, $message) = @_;
    say $message;
    $self->inc_errors;
}


sub each_source_row {
    my $self = shift;
    $self->_db_each_row($self->source_dbh, @_);
}


sub each_target_row {
    my $self = shift;
    $self->_db_each_row($self->target_dbh, @_);
}


sub map_source_rows {
    my $self = shift;
    $self->_db_map_rows($self->source_dbh, @_);
}


sub map_target_rows {
    my $self = shift;
    $self->_db_map_rows($self->target_dbh, @_);
}


sub target_commit {
    my($self) = @_;
    $self->target_dbh->commit;
}


sub source_row_count {
    my($self, $from_where) = @_;
    return $self->_db_row_count($self->source_dbh, $from_where);
}


sub _db_row_count {
    my($self, $dbh, $from_where) = @_;
    my $sql = "SELECT count(*) FROM $from_where";
    my $row = $dbh->selectrow_arrayref($sql);
    return $row->[0];
}


sub _db_connect {
    my($self, $dsn, $user, $conf_attr) = @_;
    my $attr = {
        RaiseError => 1,
        PrintError => 0,
        AutoCommit => 0,
        %$conf_attr,
    };
    return DBI->connect($dsn, $user, undef, $attr);
}


sub _db_each_row {
    my($self, $dbh, $sql, @args) = @_;
    my $handler = pop @args;

    my $sth = $dbh->prepare($sql);
    $sth->execute(@args);
    while(my $row = $sth->fetchrow_hashref) {
        $handler->($row);
    }
}


sub _db_map_rows {
    my($self, $dbh, $sql, @args) = @_;
    my $handler = pop @args;

    my @rows;
    my $sth = $dbh->prepare($sql);
    $sth->execute(@args);
    while(my $row = $sth->fetchrow_hashref) {
        push @rows, $handler->($row);
    }

    return @rows;
}


sub source_columns_for_table {
    my($self, $table) = @_;
    my $col_map = $self->source_table_columns;
    my $cols = $col_map->{$table}
        or die "Source DB has no table called '$table'";
    return @$cols;
}


sub target_columns_for_table {
    my($self, $table) = @_;
    my $col_map = $self->target_table_columns;
    my $cols = $col_map->{$table}
        or die "Target DB has no table called '$table'";
    return @$cols;
}


sub _build_unconverted {
    my($self) = @_;

    # Get a list of tables in the source DB and store as a hash so we can
    # delete table names from the hash as we convert them and should end up
    # with an empty hash at the end.

    my %tables;
    my $sql = q{
        SELECT c.relname AS table
        FROM pg_class c
        LEFT OUTER JOIN pg_namespace ns ON (ns.oid = c.relnamespace)
        WHERE ns.nspname = 'public'
        AND c.relkind = 'r'
        ORDER BY relname;
    };
    $self->each_source_row($sql, sub {
        my($row) = @_;
        $tables{ $row->{table} } = 1;
    });
    return \%tables;
}


sub _list_table_columns {
    my($self, $dbh) = @_;
    my %table;
    my $sql = q{
        SELECT
            t.relname AS table,
            c.attname AS column
        FROM pg_class t
        JOIN pg_namespace ns ON (ns.oid = t.relnamespace)
        JOIN pg_attribute c ON (t.oid = c.attrelid)
        WHERE ns.nspname = 'public'
        AND t.relkind = 'r'
        AND c.attnum > 0
        AND NOT c.attisdropped
        ORDER BY t.relname, c.attnum;
    };
    $self->_db_each_row($dbh, $sql, sub {
        my($row) = @_;
        my $tab_name = $row->{table};
        my $col_name = $row->{column};
        $table{$tab_name} ||= [];
        push @{ $table{$tab_name} }, $col_name;
    });
    return \%table;
}


__PACKAGE__->meta->make_immutable;


__END__

=head1 NAME

DBMerge - Merge records from one database into another

=head1 DESCRIPTION

This module provides a framework and support code for reading records from
one database and inserting them into another.  You would typically use it via
a wrapper script and a derived class.

Your wrapper script would define the database connection parameters and
would then call the C<do_merge()> method in the derived class.  It could be
as simple as:

  use MyAppMerge;

  MyAppMerge->do_merge(
      verbose         => 0,   # turn on for debugging
      source_dsn      => "dbi:Pg:dbname=myapp-source",
      source_db_user  => 'myapp'
      source_db_attr  => { pg_enable_utf8 => 1 },
      target_dsn      => "dbi:Pg:dbname=myapp-target",
      target_db_user  => 'myapp'
      target_db_attr  => { pg_enable_utf8 => 1 },
  );

The derived class will inherit all the good stuff from the C<DBMerge> class
and will add the necessary logic for dealing with the data in your application.
There's only one method that the wrapper class I<must> implement and additional
methods can be added to override default behaviours.  The most basic skeleton
for the derived class would be:

  package MyAppMerge;

  use Moose;
  extends 'DBMerge';


  my $table_handlers = [

      # Define how to handle each table - see below

  ];

  sub table_handlers {
      return $table_handlers;
  }

  __PACKAGE__->meta->make_immutable;

If you omit the C<table_handlers> method you can run the wrapper script
and it will connect to the source database, examine the tables and emit a
better starting skeleton that you can paste in.

The code does just use DBI so it should work with any database, however the
original target was an app running on Postgres so there are probably a number
of Postgres-specific assumptions in the code.

=head1 THE MERGE PROCESS

The basic premise is that the merge process will iterate through every table
in the source database and 'handle' it in some way (e.g.: it might copy every
row to the target database).  The C<table_handlers()> method simply returns
a data structure that defines the order in which the tables should be considered
and which handler method should be used for each table.  A number of handler
methods are provided and you can write your own if you have very specific
requirements.

Once all of the table handlers have been executed, a check is made to ensure
that every table has been 'handled' - if not, an error will be emitted for each
unhandled table.  If you only need to work with a subset of the tables you can
override the C<check_unconverted()> method with a no-op.

Finally, the error count will be checked.  If the count is non-zero then the
merge will be aborted.  If no errors have occurred then the merge will be
committed.  The default implementation does the whole merge in a single
transaction so that it either completes without error or does nothing at all
(apart from perhaps incrementing some sequences).  This should be fine for
datasets with hunderds of thousands of rows but may not be appropriate if
you have to deal with millions of rows.  You can call commit more regularly in
your handler methods if necessary.

=head1 TABLE HANDLERS

If you run the wrapper script with valid database connection details but no
implementation of the C<table_handlers()> method then you'll get a skeleton
definition like this:

  $table_handlers = [     # row_count_estimates
      al_links                => [      0 ],
      attachments             => [  3_716 ],
      auth_sources            => [      1 ],
      boards                  => [      2 ],
      burndown_days           => [      0 ],
      # ...
  ];

  sub table_handlers { return $table_handlers; }

The C<table_handlers()> method should simply return a reference to an array of
C<< table-name => [ handler-name ] >> pairs.  Some high-level things to note:

=over 4

=item *

The main data structure is an array not a hash - don't be fooled by the C<<
name => value >> pairs.  It has to be an array because the tables will need to
be 'handled' in a specific order.

=item *

The default skeleton structure simply lists every table in alphabetical order.
You will need to re-order the entries to ensure data from one table is copied
over before a later table needs to reference it.

=item *

The handler definition is in square brackets because it is also an array - any
extra values will be passed to the handler method.

=item *

The generated skeleton provides a number in the array.  This number is an
estimate of the number of rows in the table.  Providing a number instead of a
handler name is valid but does nothing - so you can run the skeleton code and
it will consider all the tables to be 'unhandled'.

=back

As you decide what the handling for each table will be, you'll change the
number in square brackets to a name and possibly adding some additional
arguments.  If for example your declaration for one table looked like this:

  status_codes      => [ 'merge_statuses', 'arg1', 'arg2' ],

Then when this table was reached in the main loop, the
C<handle_merge_statuses()> method would be called and it would be passed the
table name followed by the extra arguments, so the signature would look like
this:

  sub handle_merge_statuses {
      my $self  = shift;    # use to get access to DB handles etc
      my $table = shift;    # 'status_codes'
      my @args  = @_;       # 'arg1', 'arg2'
      # logic here
  }

Of course the whole point of this framework is that it provides a number of
pre-defined handlers which you can simply name rather than writing code.  The
supplied handlers are:

=head2 skip

You may have a number of tables that do not contain data that needs to be
copied over (perhaps because you're going to manually set up those records
in advance on the target system via the application user interface).  In
such cases, you can simply set the handler to C<'skip'>:

  sessions          => [ 'skip' ],

Having decided that a table can be skipped, you may like to add a comment or
an extra argument that documents why you decided not to copy the data:

  sessions          => [ 'skip' ], # will force users to log back in
  hot_deals         => [ 'skip', 'deals will have expired' ],

The supplied C<skip_handler()> method will silently ignore any extra arguments.

=head2 skip_empty

If you assess a table and find that it has no rows then you might use the
C<skip_empty> handler.  Like C<skip>, this handler will not copy any data,
however it will check that the table actually is empty and will emit an error
if any rows are found.  So you would use this method to document your
assumption that the table contains no rows and to ensure that you will be
notified if the situation changes and your assumption later proves to be false:

  employee_perks    => [ 'skip_empty' ],

=head2 skip_subset

Some tables contain fairly static data that might have been added during the
initial installation of the app.  You can use this handler method to assert
that every row that exists in the source database already exists in the target
database.  The <skip_subset> handler will compare all rows from the source with
all rows from the target and will emit an error if any are missing (extra rows
on the target are silently ignored):

  config_items      => [ 'skip_subset' ],

The comparison works by doing a 'SELECT *' from each table (source and target)
and turning each row into a string like C<< '1|Date format|DD/MM/YYYY' >>.
These strings are then slurped into memory and sorted alphabetically.  The two
lists (source vs target) are then diffed and missing records will be described
in diff format.  The rows being compared must be identical - right down to the
serial primary key if there is one.

=head2 build_mapping

Use this handler if you have a table that does have the same records on both
source and target, but possibly with different IDs:

  users             => [ 'build_mapping', 'email' => 'id' ],

In this case you might have manually added users to the target system in
advance of doing the data merge.  When this handler method runs, it doesn't
copy any data, instead it queries both sides and builds up a hash mapping the
id from the source table with the id of the matching record from the target
table.  In the example above, the value in the 'email' column will be used to
match records.

An error will be emitted if a record on the source side has no corresponding
record on the target side.

The result of running this method is that a mapping will be created which can
be referenced by later handlers.  The mapping name is a string of the form
C<'table_name.column_name'>.  So for example the declaration above would
produce a mapping called C<'users.id'>.  The C<'copy_rows'> handler (described
below) can map column values in other tables using this mapping.

=head2 mapping_table

This handler has a similar use case to C<'build_mapping'> (above) except this
is for situations where records cannot be automatically matched.  Instead, you
will add table to the source database which contains the ID mapping, along with
a description field.  This handler will slurp the rows from this table into a
mapping hash and will check that every row from the source table has a
corresponding entry in the mapping table.  The declaration might look like
this:

  projects          => [ 'mapping_table', 'project_desc' => 'id' ],

You might use SQL like this to create the mapping table:

  CREATE TABLE _mapping_projects (
      source_project_desc   TEXT NOT NULL,
      source_id             INTEGER NOT NULL,
      target_id             INTEGER
  );

  INSERT INTO _mapping_projects VALUES
      ('Project Myrtle', 1, 315),
      ('Moon shot',      2, 316);

The name of the mapping table is the same as the table being 'handled' except
with the added prefix '_mapping_'.

The first column in the mapping table is for documentation purposes only.  In
the event that the referenced ID does not exist in the target table, the
description column value is used in the emitted error.

=head2 check_no_errors

None of the handlers described above will add any data to the target database
(some will read from it).  You would typically declare all the non-write
handlers first and then add a handler declaration like this:

  ''                => [ 'check_no_errors' ],

This will abort with a fatal exception if any of the proceeding handlers
emitted an error.  I.e. it is only worth proceeding with subsequent handlers
if there have been no error up to this point.

=head2 copy_rows

This is the only standard handler method that will actually SELECT records from
the source database and INSERT them into the target database.  It is a
workhorse method with a number of options for customising its behaviour.  A
simple example to copy all rows in the C<'tasks'> table might be:

    tasks             => [ copy_rows => {
        order_by          => 'id',
    }],

As you can see, the C<'copy_rows'> handler takes a hash of options.  The
'order_by' option must always be specified.

The 'condition' option can be used to be more selective about which records are
SELECTed from the source database.  The value of 'condition' will be copied
verbatim into the generated SQL SELECT statement so it can make use of
sub-queries:

    tasks             => [ copy_rows => {
        condition         => "WHERE task_status <> 'CANCELLED'",
        order_by          => 'id',
    }],

Another common requirement is to use the mappings defined earlier to rewrite
some columns on the fly.  For example in cases where related records exists in
both databases, but with different ID values, column values can be rewritten
using the mapping hashes defined above, like this:

    tasks             => [ copy_rows => {
        condition         => "WHERE task_status <> 'CANCELLED'",
        order_by          => 'id',
        column_mappings   => [
            owner_user_id   => 'users.id',
            project_id      => 'projects.id',
        ],
    }],

Every column value that is not 'mapped' will simply be copied, including
serial primary key values.  In order to avoid conflicts in the target database,
you might bump a sequence to leave a pre-allocated range of available ID values.
Then you might define a method (in your derived class) to allocate IDs from
the range:

    tasks             => [ copy_rows => {
        condition         => "WHERE task_status <> 'CANCELLED'",
        order_by          => 'id',
        column_mappings   => [
            id              => 'map_task_id(id)',
            owner_user_id   => 'users.id',
            project_id      => 'projects.id',
        ],
    }],

In this example, you will be defining a C<map_task_id()> method which will be
passed the value of the 'id' column from the source database record.  If the
option was set to 'map_task_id(*)' then the method would be passed a hashref of
all the column values.

If the primary key of a record is being changed, you can use the 'make_mapping'
option to build up a new mapping to be used by later tables to refer to records
in this table:

    tasks             => [ copy_rows => {
        condition         => "WHERE task_status <> 'CANCELLED'",
        order_by          => 'id',
        column_mappings   => [
            id              => 'map_task_id(id)',
            owner_user_id   => 'users.id',
            project_id      => 'projects.id',
        ],
        make_mapping      => 'id',
    }],

This example will create a mapping called 'tasks.id'.  You can also use the
'save_mapping' option to save a permanent record of the mapping generated into
a file in JSON format:

    tasks             => [ copy_rows => {
        condition         => "WHERE task_status <> 'CANCELLED'",
        order_by          => 'id',
        column_mappings   => [
            id              => 'map_task_id(id)',
            owner_user_id   => 'users.id',
            project_id      => 'projects.id',
        ],
        make_mapping      => 'id',
        save_mapping      => 'tasks-id-mapping.json',
    }],

You can also call methods to make aribtrary changes to column values before or
after the 'column_mappings' have been applied, using the 'pre_map_method' and
'post_map_method' options.  When these methods are called, they will be
passed a hashref of column values and they can simply overwrite values in the
hash to change the data that will be written to the target database.

    tasks             => [ copy_rows => {
        condition         => "WHERE task_status <> 'CANCELLED'",
        order_by          => 'id',
        pre_map_method    => 'pre_map_tasks',
        column_mappings   => [
            id              => 'map_task_id(id)',
            owner_user_id   => 'users.id',
            project_id      => 'projects.id',
        ],
        post_map_method   => 'post_map_tasks',
        make_mapping      => 'id',
        save_mapping      => 'tasks-id-mapping.json',
    }],

Another way to handle mapping the primary key is to omit the column from the
insert statement and then build a mapping of the value that was assigned on
the target database using the 'last_insert_id' option:

    tasks             => [ copy_rows => {
        condition         => "WHERE task_status <> 'CANCELLED'",
        order_by          => 'id',
        omit_columns      => [ 'id' ],
        column_mappings   => [
            owner_user_id   => 'users.id',
            project_id      => 'projects.id',
        ],
        last_insert_id    => [ 'tasks_id_seq' => 'id' ],
        make_mapping      => 'id',
        save_mapping      => 'tasks-id-mapping.json',
    }],

This option requires that you provide the name of the sequence used to generate
the default value, as well as the column name to be be used for the mapping
(this is DBD::Pg-specific behaviour).

=head1 APOLOGIES, DISCLAIMERS, ETC

That's it for the documentation - feel free to ask questions by email or via
Github issues.

The code would probably need significant tweaking to work with a non-Postgres
database - I haven't needed to do that yet.

=head1 COPYRIGHT

Copyright 2016 Grant McLean E<lt>grantm@cpan.orgE<gt>

This library is free software; you can redistribute it and/or modify it
under the same terms as Perl itself.

=cut

