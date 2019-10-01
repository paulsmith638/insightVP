import sys,os,time,datetime,pickle
from vp_data import DataProc,Timeseries
from pv_ingest import Ingest
#from pv_database import DBsession
ingest = Ingest()

mysql_login = "psmith"
mysql_pass = "Ins#ght2019"
mysql_host = "54.173.55.254"
mysql_port = 3306
vinepair_database = "vinepair1"

#dbsession = DBsession(mysql_login,mysql_pass,mysql_host,mysql_port,vinepair_database)
#dbsession.create_lookups()

target_start="2018-01-01"
target_end="2019-09-25"

proc  = DataProc(mysql_login,mysql_pass,mysql_host,mysql_port,vinepair_database)

query = proc.dbe("SELECT pindex FROM pindex")
pindex_list = list(tup[0] for tup in query)


master_ts_dict = {}
counter=0
for pindex in set(pindex_list):
    print "GO",pindex
    d,v = proc.get_timeseries_pindex(pindex,"uniquePageviews")
    new_ts = Timeseries(d,v)
    ts_dict=master_ts_dict.get(pindex,{})
    ts_dict["uniquePageviews"] = new_ts
    master_ts_dict[pindex] = ts_dict
    counter = counter+1
    print "Stored TS data for:",counter,pindex
    if counter == 100:
        break

f = open('unique_pv_ts.pkl', 'wb')
pickle.dump(master_ts_dict, f)
f.close()
