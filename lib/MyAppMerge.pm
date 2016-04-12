package MyAppMerge;

use 5.014;
use Moose;
use Data::Dumper;

extends 'DBMerge';


my $table_handlers = [

    # Define how to handle each table

];


# Commented out because without an implementation, the base class will
# emit a useful starting suggestion to cut and paste above
#
#sub table_handlers {
#    return $table_handlers;
#}


sub pre_map_request {
    my($self, $rec) = @_;

    return 'SKIP' if $rec->{request_id} <= 0;
}


sub map_request_id {
    my($self, $request_id) = @_;

    return 100_000 + $request_id;
}


sub post_map_saved_queries {
    my($self, $rec) = @_;

    $rec->{query_name} = 'ES ' . $rec->{query_name};
}


__PACKAGE__->meta->make_immutable;
