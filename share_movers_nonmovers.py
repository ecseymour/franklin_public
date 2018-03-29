'''
for each cluster:
	* find share movers and non-movers
	* among movers: find share of moves to cluster, including  self cluster
	* start w/ acs county to county migration 2006--2010 WIHTOUT CHARACTERISTICS


NOTES:
Baker County (13007) and Kenedy County (48261) are not lised as "current counties"
in the ACS 2006-2010 data, so I can not get movers and nonmovers from them.
However, both appears as origin counties in the data (very small numbers)

'''


from pysqlite2 import dbapi2 as sql
from collections import OrderedDict
import pickle
import pandas as pd
import numpy as np

#############################################################################
#############################################################################
# connect to db w/ decennial census data and typology labels
db = "/home/eric/Documents/franklin/county_loss/data/county_population.sqlite"
con = sql.connect(db)
con.enable_load_extension(True)
con.execute('SELECT load_extension("mod_spatialite");')
# attach county flows db
cur = con.cursor()
db2 = "/home/eric/Documents/franklin/county_loss/data/county_flows.sqlite"
cur.execute('ATTACH DATABASE "{}" AS a;'.format(db2))
execute = cur.execute
fetchall = cur.fetchall

#############################################################################
#############################################################################
# create dict with geoid as key and cluster label as value
cluster_dict = OrderedDict()
cur.execute('''
	SELECT geoid10, cluster_label
	FROM omspell_clusters;
	''')
results = cur.fetchall()
for row in results:
	geoid10 = row[0]
	cluster_label = row[1].replace(' ', '')
	if "Loss" in cluster_label:
		cluster_label = "L_{}".format(cluster_label)
	else:
		cluster_label = "G_{}".format(cluster_label)
	cluster_dict[geoid10] = cluster_label

cluster_dict['46102'] = cluster_dict['46113'] # Shannon County SD renamed Oglala Lakota County in 2015 


# create list of cluster labels
cluster_list = []
cur.execute('''
	SELECT DISTINCT cluster_label
	FROM omspell_clusters;
	''')
results = cur.fetchall()
for row in results:
	cluster_label = row[0].replace(' ', '')
	if "Loss" in cluster_label:
		cluster_label = "L_{}".format(cluster_label)
	else:
		cluster_label = "G_{}".format(cluster_label)
	cluster_list.append(cluster_label)
print cluster_list
#############################################################################
#############################################################################
# get source (as opposed to destination) counties
qry = '''
SELECT GEOID10, SUBSTR(GEOID10, 1, 2) AS statefp, SUBSTR(GEOID10, 3, 5) AS countyfp, 
POP2010, EPOP2000, COUNTY
FROM county_population
WHERE EPOP2000 > 0
AND STATE NOT IN ('Alaska', 'Hawaii') --exclude AK and HI--
-- AND SUBSTR(GEOID10, 1, 2) = '46' -- REMOVE FILTERS WHEN DONE TESTING --
;
'''
execute(qry)
results = fetchall()

geoid_dict = OrderedDict()

for row in results:
	geoid10 = row[0]
	statefp = row[1]
	countyfp = row[2]
	pop2010 = row[3]
	pop2000 = row[4]
	county_name = row[5]
	geoid_dict[geoid10] = {}
	geoid_dict[geoid10]['statefp'] = statefp
	geoid_dict[geoid10]['countyfp'] = countyfp
	geoid_dict[geoid10]['name'] = county_name
	geoid_dict[geoid10]['pop2010'] = pop2010
	geoid_dict[geoid10]['pop2000'] = pop2000
	geoid_dict[geoid10]['source_cluster'] = cluster_dict[geoid10]
	geoid_dict[geoid10]['total_outmigration'] = 0
	geoid_dict[geoid10]['nonmovers'] = 0
	geoid_dict[geoid10]['movers'] = 0
	for cluster in cluster_list:
		geoid_dict[geoid10][cluster] = 0
	# for y in range(1,7,1):
	# 	geoid_dict[geoid10]['Cluster {}'.format(str(y))] = 0
#############################################################################
#############################################################################
for k, v in geoid_dict.iteritems():
	try:
		# FIND NONMOVERS AND MOVERS FOR EACH "CURRENT COUNTY"
		# collect only ONE value for each "current county" - do not aggregate
		execute('''
			SELECT nonmover_cur_cty_est, movers_cur_cty_est
			FROM ctyxcty_acs_0610
			WHERE custom_dest_5digit = ?;
			''', ([k]))
		results = cur.fetchone()
		nonmovers = results[0]
		movers = results[1]
		geoid_dict[k]['nonmovers'] += nonmovers
		geoid_dict[k]['movers'] += movers
		# get county to county migration flow
		execute('''
			SELECT custom_dest_5digit, movers_within_flow_est
			FROM ctyxcty_acs_0610
	 		WHERE custom_orig_5digit = ? AND custom_dest_5digit != ?
	 		AND cur_res_fp_sta NOT IN ('002', '015') AND CAST(cur_res_fp_sta AS INT) <= 56
	 		''', (k, k))
		results = cur.fetchall()
		for row in results:
			dest = row[0]
			flow = row[1]
			if flow != '.' and flow != '':
				geoid_dict[k][cluster_dict[dest]] += flow
	except:
		print k, v['name']


df = pd.DataFrame.from_dict(geoid_dict, orient='index')

df.to_pickle("/home/eric/Documents/franklin/github_public/temp/ACS_migration_nocharacteristics.pickle")

# grouped = df.groupby('source_cluster')[['movers', 'nonmovers']].sum()
# print  grouped['movers'] / grouped['nonmovers'] * 1000



con.close()
print "done"