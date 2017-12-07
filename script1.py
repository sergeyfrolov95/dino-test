import MySQLdb as mdb

import csv
from datetime import datetime, timedelta
import re

FILENAME = "raw_data.csv"
DELTA = timedelta(minutes=15)

con = mdb.connect('localhost', 'sergey', 'sergey1', 'dino')
cur = con.cursor()

with open(FILENAME, "r") as file:
	rows = csv.reader(file)

	insert_data = {}
	start = ''

	for row in rows:
		if row[0] == 'ts':
			continue
		if start == '':
			start = datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S,%f")
		if start > datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S,%f"):
			for key, value in insert_data.iteritems():
				cur.execute("INSERT INTO raw_data(timeframe_start, api_name, http_method, count_http_code_5xx) VALUES(%s, %s, %s, %s)", (start, key[0], key[1], value))
			insert_data = {}
			start = datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S,%f")
			continue
		if datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S,%f") - start >= DELTA:
			for key, value in insert_data.iteritems():
				cur.execute("INSERT INTO raw_data(timeframe_start, api_name, http_method, count_http_code_5xx) VALUES(%s, %s, %s, %s)", (start, key[0], key[1], value))
			insert_data = {}
			start = datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S,%f")
		else:
			if (row[1], row[2]) not in insert_data and re.match(r'5[0-9][0-9]', row[3]):
				insert_data.update({(row[1], row[2]): 1})
			elif re.match(r'5[0-9][0-9]', row[3]):
				insert_data[(row[1], row[2])] += 1
 
con.commit()
