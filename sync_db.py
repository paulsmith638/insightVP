import sys,os,time,datetime
from vp_data import DataProc
from pv_ingest import Ingest
#from pv_database import DBsession
ingest = Ingest()

mysql_login = "psmith"
mysql_pass = "Ins#ght2019"
mysql_port = 3306
vinepair_database = "vinepair1"

#dbsession = DBsession(mysql_login,mysql_pass,mysql_port,vinepair_database)
#dbsession.create_lookups()

target_start="2018-01-01"
target_end="2019-09-25"

start_dt = datetime.datetime.strptime(target_start,"%Y-%m-%d")
end_dt = datetime.datetime.strptime(target_end,"%Y-%m-%d")
target_ndays = (end_dt - start_dt).days + 1
target_dt_list =  list(start_dt + datetime.timedelta(days=i) for i in range(target_ndays))
proc  = DataProc(mysql_login,mysql_pass,mysql_port,vinepair_database)
db_session = proc.dbsession
db_session.create_lookups()
dt_with_data = proc.dt_list
dt_with_data.sort()
dt_with_data = list(dt_with_data[i] for i in range(1,len(dt_with_data)-1))
missing_days = list(set(target_dt_list) - set(dt_with_data))
missing_days.sort()
ingest.get_google_data(db_session,missing_days,db_session.pindex_lookup)
