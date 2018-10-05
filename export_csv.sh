#!/bin/bash

sql=`cat export.sql`

sqlite3 holds_table.db <<EOF
.headers on
.mode csv
.output data.csv
$sql
EOF

echo "done."
