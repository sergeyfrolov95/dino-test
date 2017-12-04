import MySQLdb as mdb
import math

def get_3_sigma(list_of_counts):
	average = sum(list_of_counts) / float(len(list_of_counts))
	res = 0
	for x in list_of_counts:
		res += (x - average)**2
	res /= len(list_of_counts)
	return (3*math.sqrt(res), average)

con = mdb.connect('localhost', 'sergey', 'sergey1', 'dino')
cur = con.cursor()

cur.execute("SELECT id, api_name, http_method, count_http_code_5xx FROM raw_data")

rows = cur.fetchall()
dict_of_sigmas = {}

for row in rows:
	if not dict_of_sigmas.get((row[1], row[2])):
		dict_of_sigmas.update({(row[1], row[2]):[int(row[3])]})
	else:
		dict_of_sigmas[(row[1], row[2])].append(int(row[3]))

for key, value in dict_of_sigmas.iteritems():
	value = get_3_sigma(value)
	dict_of_sigmas[key] = value

insert_data = {}
for row in rows:
	insert_data.update({(int(row[0]), row[1], row[2], int(row[3])): 0})
for key, value in insert_data.iteritems():
	if key[3] > dict_of_sigmas[(key[1], key[2])][0] + dict_of_sigmas[(key[1], key[2])][1]:
		insert_data[key] = 1

for key, value in insert_data.iteritems():
	cur.execute("UPDATE raw_data SET is_anomaly=%s WHERE id=%s", (value, key[0]))

con.commit()
