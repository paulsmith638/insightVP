import sys,os,time,datetime
from vp_data import DataProc
from pv_ingest import Ingest
from vp_prop import vinepair_creds
ingest = Ingest()
creds = vinepair_creds()
###################
# USER INPUT

# Update mode
# SYNC = fill in fields missing from date range
# PULL = ingest data in date range, even if already in the database
#      --> only newest copy will be kept

UPDATE_MODE = "SYNC"

# Put target tates here in YYYY-mm-dd format
# or use "TODAY" for target_end
# target_start can be an integer for # of days prior (e.g. 7 = 1 week before target_end)
target_start="2016-01-01"
target_end="TODAY"



# END USER INPUT
##################

if UPDATE_MODE not in ("SYNC","PULL"):
    print "ERROR, UPDATE_MODE must be either SYNC or PULL"
    sys.exit()

if target_end != "TODAY":
    end_dt = datetime.datetime.strptime(target_end,"%Y-%m-%d")
else:
    today = datetime.date.today()
    end_dt = datetime.datetime(year=today.year,month=today.month,day=today.day)
if type(target_start) == type(0):
    start_dt = end_dt - datetime.timedelta(days=target_start)
else:
    start_dt = datetime.datetime.strptime(target_start,"%Y-%m-%d")

    
target_ndays = (end_dt - start_dt).days + 1
target_dt_list =  list(start_dt + datetime.timedelta(days=i) for i in range(target_ndays))
proc  = DataProc(**creds)
proc.db_init()
db_session = proc.dbsession
db_session.create_lookups()


if UPDATE_MODE == "SYNC":
    #check day-by-day, each field for missing data

    missing_pv = []
    missing_sp = []
    missing_ev = []
    missing_st = []

    #check pagedata
    for field in ("sessions","pageviews","uniquePageviews","avgSessionDuration","entrances","bounceRate","exitRate"):
        print "Checking data completeness for",field
        sql = 'SELECT DISTINCT date FROM pagedata WHERE `key`="%s"' % field
        with_data= list(tup[0] for tup in db_session.session.execute(sql).fetchall())
        missing = set(target_dt_list) - set(with_data)
        missing_pv.extend(list(missing))
    #check search page results
    for field in ("clicks","ctr","impressions","position"):
        print "Checking data completeness for",field
        sql = 'SELECT DISTINCT date FROM pagedata WHERE `key`="%s"' % field
        with_data= list(tup[0] for tup in db_session.session.execute(sql).fetchall())
        missing = set(target_dt_list) - set(with_data)
        missing_sp.extend(list(missing))
    #check events results
    for field in ("scroll_events",):
        print "Checking data completeness for", field
        sql = 'SELECT DISTINCT date FROM pagedata WHERE `key`="%s"' % field 
        with_data = list(tup[0] for tup in  db_session.session.execute(sql).fetchall())
        missing = set(target_dt_list) - set(with_data)
        missing_ev.extend(list(missing))
    #check for search terms, only kept at google for 16-18mo.
    for field in ("clicks","ctr","impressions","position"):
        print "Checking data completeness for",field
        sql = 'SELECT DISTINCT date FROM searchdata WHERE `key`="%s"' % field
        with_data = list(tup[0] for tup in db_session.session.execute(sql).fetchall())
        missing = set(target_dt_list) - set(with_data)
        missing_st.extend(list(missing))

    #uniquify, omit search queries older than 540 days (about 18mo)
    #               event queries are stored for 26mo
    missing_pv = sorted(list(set(missing_pv)))
    missing_sp = sorted(list(dt for dt in set(missing_sp) if (datetime.datetime.today()-dt).days < 540))
    missing_ev = sorted(list(dt for dt in set(missing_ev) if (datetime.datetime.today()-dt).days < 800))
    missing_st = sorted(list(dt for dt in set(missing_st) if (datetime.datetime.today()-dt).days < 540))

if UPDATE_MODE == "PULL":
    missing_pv = target_dt_list
    missing_sp = target_dt_list
    missing_ev = target_dt_list
    missing_st = target_dt_list 
    
#ingest data identified above
for data_tag,target_dt_list in (("pv",missing_pv),("ev",missing_ev),("sp",missing_sp),("st",missing_st)):
    print "Data Ingest:",data_tag,len(target_dt_list)
    ingest.get_google_data(db_session,target_dt_list,db_session.pindex_lookup,targets=[data_tag,])

#remove duplicates, keeping latest added
db_session.deduplicate_sql()

