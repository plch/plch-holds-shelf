#~ this script will fetch data from the sierra postgresql database and
#~ fill a local database.

import configparser
import sqlite3
import psycopg2
import psycopg2.extras
import os
from random import randint
from datetime import datetime

# debug
import pdb

class App:

	def __init__(self):
		#~ open the config file, and parse the options into local vars
		config = configparser.ConfigParser()
		config.read('config.ini')

		# the salt used for encoding the bib record id (make sure the salt is the same going forward, or we won't be able to id unique bibs)
		self.salt = config['misc']['salt']

		# the remote database connection
		self.db_connection_string = config['db']['connection_string']
		self.pgsql_conn = None

		# the local database connection
		self.local_db_connection_string = config['local_db']['connection_string']
		self.sqlite_conn = None

		# the number of rows to iterate over
		self.itersize = int(config['db']['itersize'])

		# open the database connections
		self.open_db_connections()

		# create the table if it doesn't exist
		self.create_local_table()

		# create the temp table, and fill it with any local IDs (if there are any)
		self.create_remote_temp_tables()

		# fill the local database
		self.fill_local_db()


	#~ the destructor
	def __del__(self):
		self.sqlite_conn.commit()
		self.close_connections()
		print("done.")


	def open_db_connections(self):
		#~ connect to the sierra postgresql server
		try:
			self.pgsql_conn = psycopg2.connect(self.db_connection_string)

		except psycopg2.Error as e:
			print("unable to connect to sierra database: %s" % e)

		#~ connect to the local sqlite database
		try:
			self.sqlite_conn = sqlite3.connect(self.local_db_connection_string)
		except sqlite3.Error as e:
			print("unable to connect to local database: %s" % e)


	def close_connections(self):
		print("closing database connections...")
		if self.pgsql_conn:
			if hasattr(self.pgsql_conn, 'close'):
				print("closing pgsql_conn")
				self.pgsql_conn.close()
				self.pgsql_conn = None

		if self.sqlite_conn:
			if hasattr(self.sqlite_conn, 'close'):
				print("closing sqlite_conn")
				self.sqlite_conn.close()
				self.sqlite_conn = None


	def create_local_table(self):
		cursor = self.sqlite_conn.cursor()
		
		# create the table if it doesn't exist
		sql = u"""
		CREATE TABLE IF NOT EXISTS "data"(
			`hold_id` INTEGER,
			`local_hold_id` INTEGER,
			`hash_row` TEXT UNIQUE PRIMARY KEY, -- so we can track changes made to the row
			`placed_epoch` INTEGER,
			`patron_record_id` INTEGER,
			`patron_record_num` INTEGER,
			`record_id` INTEGER,
			`record_type_code` TEXT,
			`record_num` INTEGER,
			`item_location_code` TEXT,
			`agency_code_num` INTEGER,
			`checkin_statistics_group_code_num` INTEGER,
			`checkin_statistics_group_name` TEXT,
			`s_location_code` TEXT,
			`is_frozen` INTEGER,
			`delay_days` INTEGER,
			`expires_epoch` INTEGER,
			`status` INTEGER,
			`is_ir` INTEGER,
			`pickup_location_code` TEXT,
			`is_ill` INTEGER,
			`note` TEXT,
			`ir_pickup_location_code` TEXT,
			`ir_print_name` TEXT,
			`is_ir_converted_request` INTEGER,
			`patron_records_display_order` INTEGER,
			`records_display_order` INTEGER,
			`is_deleted` INTEGER NOT NULL DEFAULT 0,
			`deleted_epoch` INTEGER,
			`modified_epoch` INTEGER
		);
		"""
		cursor.execute(sql)
						
		self.sqlite_conn.commit()		
		cursor.close()
		cursor = None


	def create_remote_temp_tables(self):
		# open the sqlfile
		# TODO: itterate over large amounts of values, to limit the size of the query (if they get really huge)
		# sql_string = open('base_remote_temp_local_ids.sql', mode='r', encoding='utf-8-sig').read()
		sql = u"""
		--- create temp table for stat group lookups
		DROP TABLE IF EXISTS temp_stat_groups
		;

		CREATE TEMP TABLE temp_stat_groups AS
		SELECT
		s.code AS s_code,
		s.location_code AS s_location_code
		-- s.name AS s_name,
		-- l.name AS l_name -- the grouped location name (doesn't include `schk02` for example)

		FROM
		sierra_view.statistic_group_myuser as s
		-- JOIN
		-- sierra_view.location_myuser as l
		-- ON
		-- l.code = s.location_code
		;

		CREATE INDEX temp_stat_groups_code ON temp_stat_groups (s_code)
		;
		---


		DROP TABLE IF EXISTS temp_hold_ready;
		---
		CREATE TEMP TABLE temp_hold_ready AS
		SELECT
		h.id as hold_id,
		-- if we ever reset or rollover our IDs ... might want to be prepared for that
		(EXTRACT(EPOCH FROM h.placed_gmt)::INTEGER::TEXT || h.id::TEXT)::BIGINT as local_hold_id,
		MD5(CAST((h.*) AS TEXT)) AS hash_row, -- this will hash the entire hold row, so we can tell if it's different next time we try to insert it
		EXTRACT(EPOCH FROM h.placed_gmt)::INTEGER AS placed_epoch,
		h.patron_record_id,
		pr.record_num AS patron_record_num,
		h.record_id,
		r.record_type_code,
		r.record_num,
		i.location_code as item_location_code,
		i.agency_code_num,
		i.checkin_statistics_group_code_num,
		s.s_location_code,
		-- s.s_name,
		-- s.l_name,
		h.is_frozen::INTEGER,
		h.delay_days,
		EXTRACT(EPOCH FROM h.expires_gmt)::INTEGER as expires_epoch,
		h.status,
		h.is_ir::INTEGER,
		h.pickup_location_code,
		h.is_ill::INTEGER,
		h.note,
		h.ir_pickup_location_code,
		h.ir_print_name,
		h.is_ir_converted_request::INTEGER,
		h.patron_records_display_order,
		h.records_display_order,
		EXTRACT(EPOCH FROM NOW())::INTEGER as modified_epoch
		FROM
		sierra_view.hold AS h
		JOIN
		sierra_view.record_metadata as r
		ON
		r.id = h.record_id
		JOIN
		sierra_view.record_metadata as pr
		ON
		pr.id = patron_record_id
		LEFT OUTER JOIN
		sierra_view.item_record as i
		ON
		i.record_id = r.id
		AND r.record_type_code = 'i' -- there might be an off chance that record is not item
		LEFT OUTER JOIN
		temp_stat_groups as s
		ON
		s.s_code = i.checkin_statistics_group_code_num
		WHERE
		h.status IN ('i', 'j', 'b')
		;
		"""			

		with self.pgsql_conn as conn:
			with conn.cursor() as cursor:
				cursor.execute(sql)

		cursor = None
		conn = None


	def rand_int(self, length):
		#~ simple random number generator for our named cursor
		return randint(10**(length-1), (10**length)-1)


	def gen_sierra_data(self):
		#~ fetch and yield self.itersize number of rows per round
		generator_cursor = "gen_cur" + str(self.rand_int(10))

		try:
			cursor = self.pgsql_conn.cursor(name=generator_cursor,
					cursor_factory=psycopg2.extras.NamedTupleCursor)
			cursor.itersize = self.itersize # sets the itersize
			cursor.execute('SELECT * FROM temp_hold_ready;')
			# cursor.execute('SELECT * FROM temp_stat_groups;')

			rows = None
			while True:
				rows = cursor.fetchmany(self.itersize)
				if not rows:
					break

				for row in rows:
					# debug
					# pdb.set_trace()
					yield row

			cursor.close()
			cursor = None

		except psycopg2.Error as e:
			print("psycopg2 Error: {}".format(e))


	def fill_local_db(self):
		sql = """
		INSERT OR IGNORE INTO data (
			hold_id,	-- 1
			local_hold_id,	--2
			hash_row, --3
			placed_epoch,	--4
			patron_record_id,	--5
			patron_record_num,	--6
			record_id,	--7
			record_type_code,	--8
			record_num,	--9
			item_location_code,	--10
			agency_code_num, --11
			checkin_statistics_group_code_num, --12
			s_location_code,	--13
			is_frozen,	--14
			delay_days,	--15
			expires_epoch,	--16
			status,	--17
			is_ir,	--18
			pickup_location_code,	--19
			is_ill,	--20
			note,	--21
			ir_pickup_location_code,	--22
			ir_print_name,	--23
			is_ir_converted_request,	--24
			patron_records_display_order,	--25
			records_display_order,	--26
			modified_epoch	--27
		)

		VALUES
		(
			?,	--1
			?,	--2
			?,	--3
			?,	--4
			?,	--5
			?,	--6
			?,	--7
			?, 	--8
			?,	--9
			?,	--10
			?,	--11
			?,	--12
			?,	--13
			?,	--14
			?,	--15
			?,	--16
			?,	--17
			?,	--18
			?,	--19
			?,	--20
			?,	--21
			?,	--22
			?,	--23
			?,	--24
			?,	--25
			?,	--26
			?	--27
		);
		"""

		# create the cursor
		cursor = self.sqlite_conn.cursor()

		row_counter = 0
		for row in self.gen_sierra_data():
			row_counter += 1

			# do the insert
			cursor.execute(sql, list(value for value in row))
			# debug
			# pdb.set_trace()

			# commit values to the local database every self.itersize times through
			if(row_counter % self.itersize == 0):
				self.sqlite_conn.commit()
				# debug
				# pdb.set_trace()
				# print(row)
			else:
				pass


#~ run the app!
start_time = datetime.now()
print('starting import at: \t\t{}'.format(start_time))
app = App()
end_time = datetime.now()
print('finished import at: \t\t{}'.format(end_time))
print('total import time: \t\t{}'.format(end_time - start_time))