import sys,os,time,datetime,pickle
from statsmodels.tsa.seasonal import seasonal_decompose
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import Column, Integer, Float,String, DateTime,MetaData,Table
from sqlalchemy.ext.declarative import declarative_base
from pv_database import DBsession

tracked_types_csv_file="tracked_types_cats.csv"



mysql_login = "psmith"
mysql_pass = "Ins#ght2019"
mysql_port = 3306
vinepair_database = "vinepair1"
wp_database = "vp_wp_data"

#vp_db = DBsession(mysql_login,mysql_pass,mysql_port,vinepair_database)
#vp_db.ingest_wordpress_tax("vp_wp_data")
#vp_db.create_lookups()
#for k,v in vp_db.pindex_lookup.iteritems():
#    print k,v

class DataProc():
    def __init__(self,mysql_login,mysql_pass,mysql_port,vinepair_database):
        #assume initialized database for now
        self.dbsession = DBsession(mysql_login,mysql_pass,mysql_port,vinepair_database)
        self.dbsession.index_columns()
        self.dbe = self.dbsession.session.execute

    def db_init(self):
        check_tables = list(tup[0] for tup in self.dbe("SHOW TABLES").fetchall())
        for table in ('pindex','pterms','tindex','ttype'):
            if table not in check_tables:
                self.dbsession.ingest_wordpress_tax(wp_database)
        #get min/max dates and other date variables
        self.first_dt = self.dbe("SELECT MIN(date) FROM pagedata").first()[0]
        self.last_dt = self.dbe("SELECT MAX(date) FROM pagedata").first()[0]
        records = self.dbe("SELECT * FROM pagedata").first()
        if records is not None:
            self.n_days = (self.last_dt - self.first_dt).days + 1
            self.dt_list = list(self.first_dt + datetime.timedelta(days=i) for i in range(self.n_days))
            self.np_dt_list = np.array(self.dt_list,dtype='datetime64[D]')
        else:
            self.n_days = 0
            self.dt_list,self.np_dt_list = [],[]
        #get unique keys/columns
        self.data_keys = []
        key_query = self.dbe("SELECT DISTINCT `key` FROM pagedata")
        for key in key_query:
            self.data_keys.append(key[0].strip())

        
    def get_tracked_items(self,tracked_types_csv_file):
        #generate dict tindex --> tracked cat/group
        self.track_lookup={}
        track_f = open(tracked_types_csv_file,'r')
        for ln,line in enumerate(track_f):
            if ln > 0:
                fields = line.split(',')
                if len(fields) > 1:
                    cat = fields[0]
                    ids = fields[1::]
                    for tindex in ids:
                        tdig=""
                        for letter in tindex:
                            if letter.isdigit():
                                tdig=tdig+letter
                        if len(tdig)>0:
                            self.track_lookup[int(tdig)] = cat.replace('"','').strip()
        track_f.close()
        #dict keys are tracked tindices
        self.tracked_tindex = self.track_lookup.keys()

        #get tracked pages
        #dict tindex-->list of pindexes
        self.tracked_tindex_pages={}
        for pindex,tlist in self.pterm_lookup.iteritems():
            for term in tlist:
                if term in self.tracked_tindex:
                    plist = self.tracked_tindex_pages.get(term,[])
                    plist.append(pindex)
                    self.tracked_tindex_pages[term] = plist
                    


    def timeseries_as_numpy(self):
        date_str =  list(dt.strftime("%Y-%m-%d") for dt in self.dt_list)
        np_cols=["pindex","key"] + date_str
        np_fmt=[np.int64,'S256'] + list(np.float32 for i in range(self.n_days))
        np_dtype = np.dtype(zip(np_cols,np_fmt))
        np_data = np.zeros(len(self.pterm_lookup.keys())*len(self.data_keys),dtype=np_dtype)
        counter=0
        for key in self.data_keys:
            for pindex in self.pterm_lookup.keys():
                dates,values = self.get_timeseries_pindex(pindex,key)
                dv_list = zip(dates,values)
                for date,value in dv_list:
                    col = date.strftime("%Y-%m-%d")
                    np_data[counter][col] = value
                counter = counter+1
                print counter
                if counter == 1000:
                    print np_data
                    sys.exit()
            
            
    def get_timeseries_pindex(self,pindex,key):
        #np_cols=("date","count")
        #np_fmt = (np.datetime64,np.float64)
        #np_dtype = np.dtype(zip(np_cols,np_fmt))
        sql_in = 'SELECT date,count FROM pagedata WHERE pindex=%s AND `key`="%s"' % (pindex,key)
        query = self.dbe(sql_in)
        dates,values = [],[]
        for result in query:
            dates.append(result[0])
            values.append(result[1])
        return dates,values


    
    def aggregate_by_plist(self,plist,key):
        n_rows = len(plist)
        n_col = self.n_days
        np_data = np.zeros((n_rows,n_col),dtype=np.float64)
        #in case of missing data, match indices to master date list, rest will be zero
        for row_index,pindex in enumerate(plist):
            print "GETTING",pindex
            dates,values = self.get_timeseries_pindex(pindex,key)
            np_dates = np.array(dates,dtype='datetime64[D]')
            dv_list = zip(np_dates,values)
            for date,value in dv_list:
                coli = np.nonzero(self.np_dt_list==date)[0][0]
                np_data[row_index,coli] = value
        print np_data
        total = np.nansum(np_data,axis=0)
        print total
        

class Timeseries():
    def __init__(self,dates,values):
        self.set_dates(dates)
        self.set_data(values)
    def set_dates(self,dt_list):
        dt_list.sort()
        self.start_dt = dt_list[0]
        self.end_dt = dt_list[-1]
        n_days = (self.end_dt - self.start_dt).days + 1
        dt_full =list(self.start_dt + datetime.timedelta(days=i) for i in range(n_days))
        missing = np.zeros(n_days,dtype=np.bool_)
        for dti,dt in enumerate(dt_full):
            if dt not in dt_list:
                missing[dti] = True
        
        self.missing = missing
        self.invalid = np.zeros(n_days,dtype=np.bool_)
    def set_data(self,values):
        data=[]
        data = np.ones(self.missing.shape[0]) * np.nan
        data[np.invert(self.missing)] = values
        self.data = list(data)
                
    def set_invalid(self,index):
        self.invalid[index] = True
    def show(self):
        n_days = (self.end_dt - self.start_dt).days + 1
        dates = list((self.start_dt + datetime.timedelta(days=i)).strftime("%Y-%m-%d") for i in range(n_days))
        print "%12s %12s %6s %6s" % ("   DATE   ","  Value  "," Missing "," Invalid ")
        missing = self.missing
        invalid = self.invalid
        for thing in dates,missing,invalid,self.data:
            print len(thing)
        for i in range(n_days):
            print "%12s %12f %6s %6s" % (dates[i],self.data[i],missing[i],invalid[i])

        
