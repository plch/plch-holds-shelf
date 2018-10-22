#!/bin/bash
# run this script to generate last weeks results

export ISO_WEEK=`date -dlast-monday +%V`
FILENAME_PREFIX=`date -dlast-monday +%Y-w%V`
echo "generating data for ISO week number $ISO_WEEK ..."

# we have to expand the variable ISO_WEEK in the sql string ...
SQL=`cat export_with_names_iso_week.sql | envsubst`
#~ echo $SQL

sqlite3 holds_table.db <<EOF
.headers on
.mode csv
.output $FILENAME_PREFIX-filled_holds.csv
$SQL
EOF

echo "done."
