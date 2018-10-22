#!/bin/bash
# run this script to generate last weeks results

LAST_WEEK=`date -dlast-monday +%V`
SQL=`cat export_with_names.sql`

sqlite3 holds_table.db <<EOF
.headers on
.mode csv
.output data.csv
$SQL
EOF

echo "done."
