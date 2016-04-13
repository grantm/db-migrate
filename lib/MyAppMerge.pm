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

__PACKAGE__->meta->make_immutable;
