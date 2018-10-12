#!/bin/bash

sql=`cat export_with_names.sql`

sqlite3 holds_table.db <<EOF
.headers on
.mode csv
.output data.csv
$sql
EOF

echo "done."
