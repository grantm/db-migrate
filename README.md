This is a small code dump that may be useful to other people.

The purpose of this software is for 'merging' two databases where each is an
independent instance of the same app.  It works by opening a connection to the
'source' database and selecting records; then opening a connection to the
target database and inserting records.  Along the way, mappings can be applied
to account for things like primary keys needing to be changed to avoid
conflicts etc.

A simple wrapper script loads `MyAppMerge.pm` which inherits from `DBMerge.pm`.
You would need to edit files as follows:

merge_myapp
===========

Modify this file to define the database connection parameters.

MyAppMerge.pm
=============

Modify this module to define handling for each table.  The details will be
specific to your application.  (More details required here).

DBMerge.pm
==========

This file defines the framework for looping through tables and copying
records, as well as the support routines for reading, transforming and writing
the data.  Modify this module to add generic functionality you require that is
not specific to your application.

