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
                    return;
                }
            }
            if($spec->{mapper}) {
                $spec->{mapper}->($self, $old, $new);
            }
            $self->dump_record($table, $old, $new) if $self->verbose;
            if(my $method = $spec->{post_map_method}) {
                if(($self->$method($new) // '') eq 'SKIP') {
                    $spec->{rows_skipped}++;
                    return;
                }
            }
            $spec->{insert_sub}->($new);
            1;
        } or do {
            my $mesg = "$@";
            die $mesg . $self->dump_record($table, $old, $new);
        };
        if($map_col) {
            $output_map->{ $old->{$map_col} } = $new->{$map_col};
        }
        $spec->{rows_copied}++;
    };
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
