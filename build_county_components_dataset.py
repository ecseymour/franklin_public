import sqlite3 as sql
import pandas as pd
import numpy as np
from collections import OrderedDict

'''

'''
############################################################################
############################################################################
# create dict of loss counties 2000-2010
db = "/home/eric/Documents/franklin/county_loss/data/county_population.sqlite"
con = sql.connect(db)
con.text_factory=str
cur = con.cursor()

qry = '''
SELECT GEOID10, EPOP2000, POP2010
FROM county_population
WHERE EPOP2000 > 0
AND STATE NOT IN ('Alaska', 'Hawaii') --exclude AK and HI--
-- AND	POP2010 < EPOP2000
;
'''

cur.execute(qry)
results = cur.fetchall()
print "shrinking counties: {}".format(len(results))
geoid_dict = OrderedDict()
for row in results:
	geoid_dict[row[0]] = {
	'pop2000' : row[1],
	'pop2010' : row[2],
	'popchange' : round((row[2] - row[1]) * 1.0 / row[1], 2),
	'statefp'	: row[0][:2],
	'countyfp'	: row[0][2:]
	} 

con.close()
############################################################################
############################################################################
# collect components of change data
db = "/home/eric/Documents/franklin/county_loss/data/county_flows.sqlite"
con = sql.connect(db)
con.text_factory=str
cur = con.cursor()

for k, v in geoid_dict.iteritems():
	cur.execute('''
		SELECT 
		DOMESTICMIG2000 + 
		DOMESTICMIG2001 + 
		DOMESTICMIG2002 + 
		DOMESTICMIG2003 +
		DOMESTICMIG2004 +
		DOMESTICMIG2005 +
		DOMESTICMIG2006 +
		DOMESTICMIG2007 + 
		DOMESTICMIG2008 + 
		DOMESTICMIG2009 +
		DOMESTICMIG2010 AS TotDomMig,
		INTERNATIONALMIG2000 + 
		INTERNATIONALMIG2001 + 
		INTERNATIONALMIG2002 + 
		INTERNATIONALMIG2003 +
		INTERNATIONALMIG2004 +
		INTERNATIONALMIG2005 +
		INTERNATIONALMIG2006 +
		INTERNATIONALMIG2007 + 
		INTERNATIONALMIG2008 + 
		INTERNATIONALMIG2009 +
		INTERNATIONALMIG2010 AS TotIntMig,
		NETMIG2000 + 
		NETMIG2001 + 
		NETMIG2002 + 
		NETMIG2003 +
		NETMIG2004 +
		NETMIG2005 +
		NETMIG2006 +
		NETMIG2007 + 
		NETMIG2008 + 
		NETMIG2009 +
		NETMIG2010 AS TotNetMig,
		NATURALINC2000 + 
		NATURALINC2001 + 
		NATURALINC2002 + 
		NATURALINC2003 +
		NATURALINC2004 +
		NATURALINC2005 +
		NATURALINC2006 +
		NATURALINC2007 + 
		NATURALINC2008 + 
		NATURALINC2009 +
		NATURALINC2010 AS TotNatInc,
		NPOPCHG_2000 + 
		NPOPCHG_2001 + 
		NPOPCHG_2002 + 
		NPOPCHG_2003 +
		NPOPCHG_2004 +
		NPOPCHG_2005 +
		NPOPCHG_2006 +
		NPOPCHG_2007 + 
		NPOPCHG_2008 + 
		NPOPCHG_2009 +
		NPOPCHG_2010 AS TotNPopChg,
		ESTIMATEsBASE2000,
		POPESTIMATE2010,
		STNAME,
		CTYNAME
		FROM county_changes 
		WHERE STATE = ? AND COUNTY = ?;
		''', (v['statefp'], v['countyfp']))
	row = cur.fetchone()
	v['TotDomMig'] = row[0] 
	v['TotIntMig'] = row[1] 
	v['TotNetMig'] = row[2] 
	v['TotNatInc'] = row[3]
	v['TotNPopChg'] = row[4]
	v['ESTIMATEBASE2000'] = row[5]
	v['CO-POPEST2010'] = row[6]
	v['STNAME'] = row[7]
	v['CTYNAME'] = row[8]

df = pd.DataFrame.from_dict(geoid_dict, orient='index')


# # calc mig as share of loss
# df['MigShareLoss'] = 0
# # share tot neg mig is negative, i.e., "loss" is positive, divide this loss by the absolute value of pop diff 2010 - 2000
# df.loc[(df['TotNetMig'] < 0) & (df['TotNPopChg'] < 0), 'MigShareLoss'] = df['TotNetMig'] / df['TotNPopChg']

# df['NatShareLoss'] = 0
# df.loc[(df['TotNatInc'] < 0)  & (df['TotNPopChg'] < 0), 'NatShareLoss'] = df['TotNatInc'] / df['TotNPopChg']

# print df.describe()
# df.to_csv("scratch.csv", index_label="FIPS")
outFile = "/home/eric/Documents/franklin/github_public/temp/county_components.pkl"
df.to_pickle(outFile)
####################################
# create categorical vars
df['NetPopChg'] = "NotLoss"
df.loc[df['pop2010'] < df['pop2000'], 'NetPopChg'] = "Loss"

df['NatIncCat'] = "NotLoss"
df.loc[(df['TotNatInc'] < 0)  & (df['TotNPopChg'] < 0), 'NatIncCat'] = "Loss"

df['NetMigCat'] = "NotLoss"
df.loc[(df['TotNetMig'] < 0)  & (df['TotNPopChg'] < 0), 'NetMigCat'] = "Loss"

df['DomMigCat'] = "NotLoss"
df.loc[(df['TotDomMig'] < 0)  & (df['TotNPopChg'] < 0), 'DomMigCat'] = "Loss"

df['IntMigCat'] = "NotLoss"
df.loc[(df['TotIntMig'] < 0)  & (df['TotNPopChg'] < 0), 'IntMigCat'] = "Loss"
####################################
# print pd.crosstab(df['NatIncCat'], df['NetMigCat'], margins=True, rownames=['Increase'], colnames=['Migration'])

# print df.head()
# # threeway tab od nat increase, domestic and interntl migration
crossed = pd.crosstab(index=df['NatIncCat'], 
	columns=[df['DomMigCat'], df['IntMigCat']],
	margins=True)

# print crossed


# xtab = pd.crosstab(df['NatIncCat'], df['NetMigCat'], margins=True, normalize='all', rownames=['Increase'], colnames=['Migration'])

# print xtab

# xtab.to_csv('county_components_xtab.csv')

# print df.describe()
# df.to_csv("scratch.csv", index_label="FIPS")
# df.to_pickle("county_components.pkl")

con.close()

print "done"