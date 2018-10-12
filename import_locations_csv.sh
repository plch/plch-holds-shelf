#!/bin/bash

# this doesn't seem to work currently, but leaving it as an example to import fresh data
cat locations.csv | cut --complement -f 3 > temp.csv

sqlite3 holds_table.db <<EOF
DROP TABLE IF EXISTS locations;
CREATE TABLE locations (`code` TEXT UNIQUE PRIMARY KEY, `name` TEXT);
.mode csv
.separator "\t"
.import temp.csv locations
.quit
EOF

rm temp.csv

echo "done."
