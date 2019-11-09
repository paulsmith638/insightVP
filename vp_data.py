import sys,os,time,datetime,pickle,csv,re
#from statsmodels.tsa.seasonal import seasonal_decompose
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import Column, Integer, Float,String, DateTime,MetaData,Table
from sqlalchemy.ext.declarative import declarative_base
from pv_database import DBsession
import pandas as pd


class DataProc():
    def __init__(self,mysql_login,mysql_pass,mysql_host,mysql_port,database,load_cache=True):
        #assume initialized database for now
        self.dbsession = DBsession(mysql_login,mysql_pass,mysql_host,mysql_port,database)
        self.dbsession.index_columns()
        self.dbe = self.dbsession.session.execute
        self.index_mat = None
        if load_cache:
            if os.path.isfile("data_cache.pkl"):
                f = open("data_cache.pkl",'rb')
                self.data_cache = pickle.load(f)
                f.close()
                n_rec = len(self.data_cache)
                print "Loaded %g records from existing data cache" % n_rec
            else:
                print "No data cache present, using DB connection for all records"
                self.data_cache = {}
        else:
            self.data_cache = {}

    def db_init(self):
        check_tables = list(tup[0] for tup in self.dbe("SHOW TABLES").fetchall())
        for table in ('pindex','pterms','tindex','ttype'):
            if table not in check_tables:
                print "UPDATING DB",table
                self.dbsession.ingest_wordpress_tax(wp_database)
        #get min/max dates and other date variables
        self.first_dt = self.dbe("SELECT MIN(date) FROM pagedata").first()[0]
        self.last_dt = self.dbe("SELECT MAX(date) FROM pagedata").first()[0]
        print "DATABASE contains data from %s to %s" % (self.first_dt.strftime("%Y-%m-%d"),
                                                        self.last_dt.strftime("%Y-%m-%d"))
        records = self.dbe("SELECT * FROM pagedata LIMIT 1").first()
        if records is not None:
            self.n_days = (self.last_dt - self.first_dt).days + 1
            self.dt_list = list(self.first_dt + datetime.timedelta(days=i) for i in range(self.n_days))
        else:
            self.n_days = 0
            self.dt_list,self.np_dt_list = [],[]
        #get unique keys/columns
        self.data_keys = []
        key_query = self.dbe("SELECT DISTINCT `key` FROM pagedata")
        for key in key_query:
            print "DATA available for key:",key[0]
            self.data_keys.append(key[0].strip())

            
    def get_timeseries_pindex(self,pindex,key):
        """
        Fetches single time series for a given pindex and key (e.g. pageviews)
        Updates a cache for repeat queries

        """
        cur_pindex_data = self.data_cache.get(pindex,{})
        if key in cur_pindex_data.keys():
            #print "Fetching %g %s timeseries from cache" % (pindex,key)
            return cur_pindex_data[key]
        else:
            sql_sub = 'SELECT MAX(id) as mid,date FROM pagedata WHERE pindex=%s AND `key`="%s" GROUP BY date' %  (pindex,key)
            sql_in = "WITH targets AS (%s) SELECT targets.date,count FROM targets INNER JOIN pagedata ON targets.mid = pagedata.id" % sql_sub
            #print "Fetching %g %s timeseries from DATABASE" % (pindex,key)
            query = self.dbe(sql_in).fetchall()
            din = list(res[0] for res in query)
            vin = list(res[1] for res in query)
            alld = zip(din,vin)
            alld.sort(key = lambda t:t[0])
            dates = list(t[0] for t in alld)
            values = list(t[1] for t in alld)
            cur_pindex_data[key] = (dates,values)
            self.data_cache[pindex] = cur_pindex_data
            return dates,values

    def get_all_bykey(self,key,pindex_list):
        """

        """
        sql_pindex_list = ",".join(list(str(pindex) for pindex in pindex_list))
        sql_sub = 'SELECT MAX(id) as mxid,date,pindex FROM pagedata WHERE `key`="%s" AND pindex in (%s) GROUP BY date,pindex' % (key,sql_pindex_list)
        sql_in = 'WITH getmax AS (%s) SELECT pagedata.date,pagedata.pindex,pagedata.count FROM pagedata JOIN getmax ON pagedata.id=getmax.mxid' % sql_sub
        query = self.dbe(sql_in).fetchall()
        print "RETREIVED %g %s records in batch from database" % (len(query),key)
        dates_dict = {}
        val_dict = {}
        for res in query:
            date_dt,pindex,val = res
            ts_dates = dates_dict.get(pindex,[])
            ts_dates.append(date_dt)
            ts_vals = val_dict.get(pindex,[])
            ts_vals.append(val)
            dates_dict[pindex] = ts_dates
            val_dict[pindex] = ts_vals
        all_dat = {}
        for pindex in dates_dict.keys():
            all_dat[pindex] = (dates_dict[pindex],val_dict[pindex])
        return all_dat
            
    

    def aggregate_by_plist(self,plist,key,prefilter=False):
        n_rows = len(plist)
        print "Loading/Fetching %g timeseries for %s" % (n_rows,key)
        n_col = self.n_days
        np_data = np.zeros((n_rows,n_col),dtype=np.float64)
        np_dt_list = np.array(self.dt_list,dtype='datetime64[D]')
        #in case of missing data, match indices to master date list, rest will be zero
        for row_index,pindex in enumerate(plist):
            dates,values = self.get_timeseries_pindex(pindex,key)
            if prefilter:
                ts = TimeSeries(dates,values)
                old_values = ts.data
                new_values = ts.rolling_median_filter()
                diff = np.abs(np.array(old_values)-np.array(new_values))
                mean = np.nanmean(new_values)
                n_diff = np.count_nonzero(diff>mean)
                print "   --> filtered %d points" % n_diff
                values = new_values
                dates = ts.dates
            np_dates = np.array(dates,dtype='datetime64[D]')
            dv_list = zip(np_dates,values)
            for date,value in dv_list:
                coli = np.nonzero(np_dt_list==date)[0][0]
                np_data[row_index,coli] = value
        total = np.nansum(np_data,axis=0)
        output_dates = list(ndt.tolist() for ndt in np_dt_list)
        return list(total),output_dates

    def get_index_matrix(self):
        """
        Generates a numpy boolean matrix for all tracked pages
        row is pindex,column is tracked term/tab
        creates dictionaries for 2-way lookups
        """
        
        if self.dbsession.pindex_lookup is None:
            self.dbsession.create_lookups()
        pindex_list = self.dbsession.pterm_lookup.keys()
        pindex_list.sort()
        tindex_list = []
        for pindex,tlist in self.dbsession.pterm_lookup.iteritems():
            tindex_list.extend(tlist)
        tindex_list = list(set(tindex_list))
        tindex_list.sort()
        n_rows = len(pindex_list)
        n_col = len(tindex_list)
        imatrix = np.zeros((n_rows,n_col),dtype=np.bool_)
        i2p = dict(list((pi,pindex) for pi,pindex in enumerate(pindex_list)))
        p2i = dict(list((pindex,pi) for pi,pindex in enumerate(pindex_list)))
        i2t = dict(list((ti,tindex) for ti,tindex in enumerate(tindex_list)))
        t2i = dict(list((tindex,ti) for ti,tindex in enumerate(tindex_list)))
        
        for pindex,tlist in self.dbsession.pterm_lookup.iteritems():
            rowi = p2i[pindex]
            for tindex in tlist:
                coli = t2i[tindex]
                imatrix[rowi,coli] = True
        self.index_mat = imatrix
        self.p2i = p2i
        self.i2p = i2p
        self.t2i = t2i
        self.i2t = i2t


    def get_plist(self,tindex):
        if self.index_mat is None:
            self.get_index_matrix()
        ti = self.t2i[tindex]
        pi_list = list(np.nonzero(self.index_mat[:,ti] == True)[0])
        plist = list(self.i2p[i] for i in pi_list)
        return plist
        
    def get_tlist(self,pindex):
        if self.index_mat is None:
            self.get_index_matrix()
        pi = self.p2i[pindex]
        ti_list = list(np.nonzero(self.index_mat[pi] == True)[0])
        tlist = list(self.i2t[i] for i in ti_list)
        print tlist



class TsArray():
    def __init__(self,series_list,dt_list):
        """
        Class for holding numpy arrays, each containing multiple timeseries
        All arrays in the object must conform to the same indexing scheme where:
        Row = timeseries
        Colunm = date
        for both row and column, forward and backward lookups are created that
           relate columns to pindex or groups and columns to dt objects
        Many arrays can be added and are referenced as object.arrays["array_name"]
        """
        self.arrays = {}
        dt_in=list(dt for dt in dt_list) #make copy
        dt_in.sort()
        if len(series_list) == 0 or len(dt_list) == 0: #allow for empty list instantiations
            self.start_dt=None
            self.end_dt=None
            self.n_days=0
            self.dt_list=[]
            self.n_rows = 0
        else:
            self.start_dt = dt_in[0]
            self.end_dt = dt_in[-1]
            #total days in interval, even if missing
            n_days = (self.end_dt - self.start_dt).days + 1
            days_per_value = float(n_days)/len(dt_list)
            if days_per_value > 10: #working with months
                self.n_days = (self.end_dt.year - self.start_dt.year) * 12 + self.end_dt.month - self.start_dt.month+1
                self.dt_list = list(dt for dt in dt_in) #no missing months present?
            else:
                self.n_days = (self.end_dt - self.start_dt).days + 1
                self.dt_list = list(self.start_dt + datetime.timedelta(days=i) for i in range(self.n_days))
            self.n_rows = len(series_list)
        p2r,r2p,d2c,c2d = {},{},{},{}
        for pi,series in enumerate(series_list):
            p2r[series] = pi
            r2p[pi] = series
        for ci,date_dt in enumerate(self.dt_list):
            d2c[date_dt] = ci
            c2d[ci] = date_dt
        self.p2r = p2r
        self.r2p = r2p
        self.d2c = d2c
        self.c2d = c2d
        self.dstr = list(dt.strftime("%Y-%m-%d") for dt in self.dt_list)

    def new_array(self,type=None):
        #must be allowed np type (int,bool_,etc)
        #default is float32 set to NaN
        #others are initialized to zero
        n_rows = self.n_rows
        n_cols = len(self.dt_list)
        if type is None:
            array = np.ones((n_rows,n_cols),dtype=np.float32) * np.nan
        else:
            array = np.zeros((n_rows,n_cols),dtype=type) 
        return array

    def add_array(self,array,name):
        #ref the array to the calling object
        self.arrays[name] = array

    def insert_ts(self,array_name,series,dates,values):
        #add a single timeseries to named array
        row = self.p2r.get(series,None)
        if row is None:
            print "ERROR, series %g not found in array!"
            return
        if len(dates) == 0 or len(values) == 0:
            return #no data to insert
        if type(dates[0]) != datetime.datetime:
            try:
                dates = list(datetime.datetime.strptime(date,"%Y-%m-%d") for date in dates)
            except:
                print "ERROR, dates not in datetime or YYYY-mm-dd format?"
                return
        if len(dates) != len(values):
            print "ERROR, dates and values do not match!"
            return
        target_array = self.arrays.get(array_name,None)
        if target_array is None:
            target_array = self.new_array()
            self.add_array(target_array,array_name)
        dzip = zip(dates,values)
        dzip.sort(key = lambda t:t[0])
        valid_dates =  list(dzt[0] for dzt in dzip if dzt[0] in self.dt_list)
        valid_values = list(dzt[1] for dzt in dzip if dzt[0] in self.dt_list)
        cols =  list(self.d2c[dt] for dt in valid_dates)
        target_array[row,cols] = valid_values
        
    def insert_by_pindex(self,proc,array_name,pindex,key):
         d,v = proc.get_timeseries_pindex(pindex,key)
         self.insert_ts(array_name,pindex,d,v)


    def insert_by_dict(self,array_name,data_dict):
        """
        Takes a dict of format series-->(d,v) and inserts into array_name
        If array doesn't exist, created.  If present, values are overwritten
        """
        all_series = data_dict.keys()
        tracked_series = self.p2r.keys()
        to_insert = list(set(all_series) & set(tracked_series))
        target_array = self.arrays.get(array_name,None)
        if target_array is None:
            target_array = self.new_array()
            self.add_array(target_array,array_name)
        for series in to_insert:
            d,v = data_dict[series]
            self.insert_ts(array_name,series,d,v)




    def merge_tsarray(self,array_to_add):
        new_dates = array_to_add.dt_list
        all_dates = list(set(self.dt_list + new_dates))
        all_dates.sort()
        new_pindex = array_to_add.p2r.keys()
        all_pindex = list(set(self.p2r.keys() + new_pindex))
        new_array = TsArray(all_pindex,all_dates,name=self.name)
        old_dates = self.dt_list
        for i in range(self.array.shape[0]):
            pindex = self.r2p[i]
            dval = list(self.array[i])
            new_array.insert_ts(pindex,old_dates,dval)
        #overwrites pre-existing data with new data
        for pindex in new_pindex:
            ri = array_to_add.p2r[pindex]
            dval = list(array_to_add.array[ri])
            new_array.insert_ts(pindex,new_dates,dval)
        return new_array


    
    def store_array(self,name):
        file_stem = name.strip().lower()
        filename = "timeseries_array_"+file_stem+".pkl"
        f = open(filename,'wb')
        pickle.dump(self,f)
        print "SAVED timeseries into file %s" % filename
        f.close()

    @staticmethod
    def load_array(name):
        file_stem=name.strip().lower()
        filename = "timeseries_array_"+file_stem+".pkl"
        if os.path.isfile(filename):
            f = open(filename,'rb')
            new_array = pickle.load(f)
            f.close()
            print "Loaded timeseries array from file %s" % filename
        else:
            print "File %s not present! Returning empty TsArray" % filename
            new_array = TsArray([],[])
        return new_array

    
    def show(self):
        n_rows,n_col = self.n_rows,len(self.dt_list)
        n_views = len(self.arrays)
        print "Timeseries Array Summary:"
        print "   Total Views  = ",n_views
        print "   Total Series = ",n_rows
        print "   Total Dates  = ",n_col
        print "   Date Range   = ",self.start_dt.strftime("%Y-%m-%d"),self.end_dt.strftime("%Y-%m-%d")
        for array_name,array in self.arrays.iteritems():
            name = array_name
            nnan = np.count_nonzero(np.isnan(array))
            print "      Array Name   = ",name
            print "         NaN values= ",nnan

    def series_summary(self,source_array,series_id):
        series = np.array(self.arrays[source_array][self.p2r[series_id]])
        nnan = np.count_nonzero(np.isnan(series))
        nnz = np.count_nonzero(series) - nnan
        if nnz == 0:
            return "Series %16s has no non-zero data" % str(series_id)
        mean = np.nanmean(series)
        median = np.nanmedian(series)
        std = np.nanstd(series)
        maxv = np.nanmax(series)
        minv = np.nanmin(series)
        maxv_dt=self.c2d[np.nonzero(series==maxv)[0][0]]
        minv_dt=self.c2d[np.nonzero(series==minv)[0][0]]
        min_date=minv_dt.strftime("%Y-%m-%d")
        max_date=maxv_dt.strftime("%Y-%m-%d")
        sname = str(series_id)
        fmtstr = ('Series {:^16}: n>0={:4} MAX {:8.1f} on {:10s} MIN {:5.1f} on {:10s} MEAN {:6.1f} MED {:6.1f} STDEV {:>5.1f}')
        outstr = fmtstr.format(sname,nnz,maxv,max_date,minv,min_date,mean,median,std)
        return outstr

class TimeSeries():
    def __init__(self,dates,values,name="Series"):
        if len(dates) != len(values):
            print "WARNING: date/value mismatch!"
            #if len(dates) > len(values):
            #    for i in range(len(dates) - len(values)):
            #        values.append(0.0) # zero pad 
            #if len(values) > len(dates):
            #    values=values[0:len(dates)] # truncate
        dat = zip(dates,values)
        dat.sort(key = lambda x:x[0])
        dates = list(dv[0] for dv in dat)
        values = list(dv[1] for dv in dat)

        self.set_dates(dates)
        self.set_data(values)
        self.name = name
        
    def set_dates(self,dt_list):
        if len(dt_list) == 0:
            self.dates = []
            self.missing = []
            self.start_dt = None
            self.end_dt = None
            self.n_days = None
            return
        self.start_dt = dt_list[0]
        self.end_dt = dt_list[-1]
        self.n_days = (self.end_dt - self.start_dt).days + 1
        self.dates = self.get_all_dates() #fill in all dates
        self.missing = np.zeros(self.n_days,dtype=np.bool_)
        for dti,dt in enumerate(self.dates):
            if dt not in dt_list:
                self.missing[dti] = True
                
    def set_data(self,values):
        if len(values) == 0:
            self.data=[]
            return
        data = np.ones(self.missing.shape[0]) * np.nan
        data[np.invert(self.missing)] = values
        self.data = list(data)
 
    def get_all_dates(self):
        dt_full =list(self.start_dt + datetime.timedelta(days=i) for i in range(self.n_days))
        return dt_full

    #no longer used
    def median_filter(self,threshold=5):
        signal = np.array(self.data)
        difference = np.abs(signal - np.median(signal))
        median_difference = np.median(difference)
        if median_difference == 0:
            s = 0
        else:
            s = difference / float(median_difference)
        mask = s > threshold
        print "FILTER",np.count_nonzero(mask)
        signal[mask] = np.median(signal)
        return list(signal)

    #no longer used
    def rolling_median_filter(self,window=11,sigcut=10):
        if len(self.data) == 0:
            return []
        rolling_median=[]
        np_dat = np.array(self.data)
        tails = int(window/2)
        for index_window in range(0,len(self.data)-2*tails,1):
            win_dat = np_dat[index_window:index_window+window]
            win_med = float(np.nanmedian(win_dat))
            rolling_median.append(win_med)
            if index_window < tails:
                rolling_median.append(win_med)
        for i in range(tails):
            rolling_median.append(win_med)
        rm_np = np.array(rolling_median)
        diff = np_dat - rm_np #only positive differences (spikes)
        threshold = np.nanstd(self.data) * sigcut
        mask = diff > threshold
        output = np.array(self.data)
        output[mask] = rm_np[mask]
        return list(output)
    
    def fill_nan(self,list_in):
        dat = np.array(list_in)
        mask = dat == np.nan
        median = np.nanmedian(dat)
        dat[mask] = median
        return list(dat)

    def arima_model(self,dates,values):
        if len(values) == 0:
            return [],[],[]
        pdf = pd.DataFrame(values)
        pdf.index = pd.to_datetime(dates)
        result = seasonal_decompose(pdf, model='multiplicative',two_sided=False)
        trend =  result.trend[0].tolist()
        resid = result.resid[0].tolist()
        seasonal = result.seasonal[0].tolist()
        """
        testing code for parameter tuning
        from statsmodels.tsa.statespace.sarimax import SARIMAX
        from matplotlib import pyplot as plt
        train = pdf.ix[0:365,0]
        test = pdf.ix[365:,0]
        sarima_model = SARIMAX(train, order=(0, 1, 2,4,8,16), seasonal_order=(0, 1, 2, 12,52), enforce_invertibility=False, enforce_stationarity=False)
        sarima_fit = sarima_model.fit()

        sarima_pred = sarima_fit.get_prediction(test.index[0], test.index[-1])
        predicted_means = sarima_pred.predicted_mean #+ test.rolling(12).mean().dropna().values
        predicted_intervals = sarima_pred.conf_int(alpha=0.25)
        lower_bounds = predicted_intervals['lower y']# + df.data.iloc[365:,0].rolling(12).mean().dropna().values
        upper_bounds = predicted_intervals['upper y']# + df.data.iloc[365:,0].rolling(12).mean().dropna().values

        sarima_rmse = np.sqrt(np.mean(np.square(test.values - sarima_pred.predicted_mean.values)))

        fig, ax = plt.subplots(figsize=(12, 4))
        ax.plot(pdf.index, pdf.values)
        ax.plot(test.index, test.values)# + pdf.iloc[365:,0].rolling(12).mean().dropna().values, label='truth')
        ax.plot(test.index, predicted_means, color='#ff7823', linestyle='--', label="prediction (RMSE={:0.2f})".format(sarima_rmse))
        ax.fill_between(test.index, lower_bounds, upper_bounds, color='#ff7823', alpha=0.3, label="confidence interval (95%)")
        ax.legend();
        ax.set_title("SARIMA");
        plt.show()
        sys.exit()
        """
        #result.plot()
        #pyplot.show()
        return trend,resid,seasonal
    
    def show(self):
        if self.end_dt is None:
            print "No data in series!"
            return
        n_days = (self.end_dt - self.start_dt).days + 1
        dates = list((self.start_dt + datetime.timedelta(days=i)).strftime("%Y-%m-%d") for i in range(n_days))
        print "%12s %12s %6s %6s" % ("   DATE   ","  Value  "," Missing "," Invalid ")
        missing = self.missing
        for i in range(n_days):
            print "%12s %12f %6s" % (dates[i],self.data[i],missing[i])

            


        
class AggScheme():
    def __init__(self):
        self.scheme = {"name":"Null",
                       "groups":[]}
            
    def get_agg_scheme(self,scheme_name,pterm_lookup,csv_in):
        self.pterm_lookup = pterm_lookup
        self.scheme["name"] = scheme_name
        """
        Reads in a csv file with the following format:
        group_name,tracked_tag1,tracked_tag2,tracked_tag3, etc.
        positive values are included, negative values are excluded
        returns a dictionary with name and groups

        groups is list of dictionaries:
             keys: group_name=str,group_master=str,group_pages=list of pindex
        """
        rev_look = {}
        track_lookup={}
        with open(csv_in,'r') as track_f:
            reader = csv.reader(track_f,delimiter=',')
            header = next(reader)
            hd_str = list(s.upper() for s in header)
            if "MASTER" in hd_str:
                rows = [(row[0].upper().strip(), row[1].upper().strip(), list(int(row[i]) for i in range(2,len(row)) if len(row[i]) > 0)) for row in reader]
            else:
                rows = [(row[0].upper().strip(), "NONE", list(int(row[i]) for i in range(1,len(row)) if len(row[i]) > 0)) for row in reader]
        track_lookup = {}
        for track_t in rows:
            track_name = track_t[0]
            track_master = track_t[1]
            tindex_include = list(tindex for tindex in track_t[2] if tindex > 0)
            tindex_exclude = list(-tindex for tindex in track_t[2] if tindex < 0)
            track_lookup[track_name] = {"include":tindex_include,"exclude":tindex_exclude,"master":track_master}
        self.track_lookup = track_lookup
        self.masters = {}
        tracked_tindex = []
        for tname,tdict in track_lookup.iteritems():
            for tindex in tdict["include"]:
                tracked_tindex.append(tindex)
            master = tdict["master"]
            master_dict = self.masters.get(master,{"terms":[],"tindex":[]})
            for tindex in tdict["include"]:
                master_dict["tindex"].append(tindex)
                master_dict["terms"].extend(list(term for term,td2 in track_lookup.iteritems() if tindex in td2["include"]))
            self.masters[master] = master_dict
        self.tracked_tindex = list(set(tracked_tindex))
        #get tracked pages for each tindex
        #essentially, invert the pterm lookup
        self.tracked_tindex_pages={}
        for pindex,tlist in self.pterm_lookup.iteritems():
            for tindex in tlist:
                if tindex in self.tracked_tindex:
                    plist = self.tracked_tindex_pages.get(tindex,[])
                    plist.append(pindex)
                    self.tracked_tindex_pages[tindex] = plist
        for gname,tdict in track_lookup.iteritems():
            group_name = gname
            group_master = tdict["master"]
            tracked_pages = []
            excluded_pages = []
            for tindex in tdict["include"]:
                tracked_pages = tracked_pages + self.tracked_tindex_pages.get(tindex,[])
            for tindex in tdict["exclude"]:
                excluded_pages = excluded_pages +self.tracked_tindex_pages.get(tindex,[])
            tracked_pages = set(tracked_pages)
            excluded_pages = set(excluded_pages)
            final_pages = list(tracked_pages - excluded_pages)
            if len(self.scheme["groups"]) == 0:
                gdict = {"group_name":group_name,"group_pages":final_pages,"group_master":group_master}
                self.scheme["groups"].append(gdict)
            else:
                existing_groups = list(gdict["group_name"] for gdict in self.scheme["groups"])
                if group_name in existing_groups:
                    for gdict in self.scheme["groups"]:
                        if gdict["group_name"] == group_name:
                            gdict["group_pages"] = list(set(gdict["group_pages"] + final_pages))
                else:
                    gdict = {"group_name":group_name,"group_pages":final_pages,"group_master":group_master}
                    self.scheme["groups"].append(gdict) 
                    

    def show(self,full=False,slug_lookup=None):
        scheme_name = self.scheme["name"]
        n_groups = len(self.scheme["groups"])
        print "Scheme %s contains %d groups:" % (scheme_name,n_groups)
        for gn,gdict in enumerate(self.scheme["groups"]):
            group_name = gdict["group_name"]
            group_pages = gdict["group_pages"]
            group_master = gdict["group_master"]
            n_pages = len(group_pages)
            print "    Group: %20s = %8s contains %5d pages." % (group_name,group_master,n_pages)
            if full and slug_lookup is not None:
                for page in group_pages:
                    print "       --> Group %16s contains page %5g with title %s" % (group_name,page,slug_lookup.get(page,"Unknown"))
                       
    def get_page_weights(self,proc):
        """
        Two weighting methods are used:
        For cosweights, pages are weighted as follows:
        1) all pages for that contain a tracked term are grouped, the sum of all tracked terms is 
           taken as a vector that is normalized.  This vector represents the global vector for all pages.
        2) the same vector is calculated for all pages in an aggregation group
        3) the same vector is calculated for a given page
        4) the raw page weight is the difference between the cosine of an individual page and 
           the cosine of the page to the global vector for all pages
        5) the raw weight is clipped to zero and the resulting values normalized (max=1.0) 
        For inv_weights, pages are weighted as follows:
        tag weights are assigned a proportion of total tracked tags on a page
        e.g. if a page has 4 tracked tags, each tag gets weight=0.25
        """
        print "Calculating page weights"
        mat = proc.index_mat.copy()
        #exclude master categories from weight tallies?
        master_tindex = self.masters["MASTER"]["tindex"]
        tracked_terms = list(term for term in self.tracked_tindex)
        #get all tracked terms as matrix indexes
        tracked_mati = list(proc.t2i[term] for term in tracked_terms)
        tracked_mask = np.array(list(index in tracked_mati for index in range(mat.shape[1])))
        has_track = np.nansum(mat[:,tracked_mask],axis=1) > 0
        tracked_pages = mat[has_track,:]
        tracked_pages = tracked_pages[:,tracked_mask]
        #net unit vector for all tracked pages
        all_page_sum = np.nansum(tracked_pages,axis=0)
        ap_len = np.linalg.norm(all_page_sum)
        all_page_vect = all_page_sum/ap_len

        #dict of weights/agg_groups
        agg_groups = list(gdict["group_name"] for gdict in self.scheme["groups"])
        master_groups = list(gn for gn in agg_groups if gn in self.masters["MASTER"]["terms"])
        tracked_pindex = list(proc.i2p[i] for i in np.nonzero(has_track)[0])
        self.weight_matrix = np.zeros((len(tracked_pindex),len(agg_groups)))
        self.w_p2r = dict(list((pindex,pi) for pi,pindex in enumerate(tracked_pindex)))
        self.w_r2p = dict(list((pi,pindex) for pi,pindex in enumerate(tracked_pindex)))
        self.w_g2c = dict(list((group,ci) for ci,group in enumerate(agg_groups)))
        self.w_c2g = dict(list((ci,group) for ci,group in enumerate(agg_groups)))

        for gdict in self.scheme["groups"]:
            group_name = gdict["group_name"]
            group_master = gdict["group_master"]
            matc = self.w_g2c[group_name]
            group_pi = list(proc.p2i[pindex] for pindex in gdict["group_pages"])
            group_submat = mat[group_pi,:]
            group_submat = group_submat[:,tracked_mask]
            group_page_sum = np.nansum(group_submat,axis=0)
            gp_len = np.linalg.norm(group_page_sum)
            group_page_vect = group_page_sum/gp_len
            group_all_sim = np.dot(group_page_vect,all_page_vect)
            for pindex in gdict["group_pages"]:
                page_row = mat[proc.p2i[pindex]].copy()
                page_row = page_row[tracked_mask]
                page_total = np.nansum(page_row).astype(np.float64)
                pr_len = np.linalg.norm(page_row)
                page_vect = page_row/pr_len
                cos1 = np.dot(all_page_vect,page_vect)
                cos2 = np.dot(group_page_vect,page_vect)
                net_diff = np.clip(cos2-group_all_sim,0.05,1.0)
                matr = self.w_p2r.get(pindex,None)
                if matr is None:
                    continue
                self.weight_matrix[matr,matc] = net_diff
        #agg_schemes without master categories first
        master_ci = list(self.w_g2c[group] for group in master_groups)
        master_row_lists = list(list(np.nonzero(self.weight_matrix[:,ci])[0] for ci in master_ci))
        for ci in master_ci:
            self.weight_matrix[:,ci] = 0.0
        row_sum = np.nansum(self.weight_matrix,axis=1)
        row_mask = row_sum > 0.0
        row_weighted = np.nansum(self.weight_matrix > 0,axis=1)
        row_invw = np.array(list(1.0/elem if elem > 0.0 else 0.0 for elem in row_weighted))
        nz_weights = self.weight_matrix > 0.0
        self.weight_matrix_simple = np.add(np.zeros(self.weight_matrix.shape),row_invw[:,None])
        self.weight_matrix_simple[np.invert(nz_weights)] = 0.0

        self.weight_matrix[row_mask] = np.divide(self.weight_matrix[row_mask],row_sum[row_mask][:,None])
        for cn,ci in enumerate(master_ci):
            self.weight_matrix[master_row_lists[cn],ci] = 1.0
            self.weight_matrix_simple[master_row_lists[cn],ci] = 1.0

    def get_tracked_pages(self):
        tracked_pindex = []
        for gdict in self.scheme["groups"]:
            tracked_pindex.extend(gdict["group_pages"])
        uniq_pindex = list(set(tracked_pindex))
        uniq_pindex.sort()
        return uniq_pindex

class NullLog(object):
    #hack to have a place to dump numpy errors
    def write(self,error):
        pass

    
class AggFunc():
    """
    Collection of functions for aggregating, weighting, and filtering TimeSeries objects
    """
    def __init__(self):
        pass


    def flag_outliers(self,agg_scheme,tsa,source_array,cutoff=0.5,sigcut=10.0,hardcut=5000):
        """
        Test each individual series in each aggregation group agains the group total
        check for values above three cutoffs: 1) more than (cutoff) of daily 
        agg total, 2) more than (sigcut) stddev above timeseries values, 3) more than (hardcut) absvalue
        returns a new array with flagged data points set to series medians
        """
        print "Performing Spike Detection"
        raw_data = tsa.arrays[source_array]
        outlier_subs = tsa.new_array(type=np.float32)
        log = NullLog()
        error_handler = np.seterrcall(log)
        np.seterr(all='log')

        for gdict in agg_scheme.scheme["groups"]:
            gname = gdict["group_name"]
            gmaster = gdict["group_master"]
            group_pages = gdict["group_pages"]
            gindex = list(tsa.p2r[pindex] for pindex in group_pages if pindex in tsa.p2r)
            agg_ts = raw_data[gindex,:]
            daily_totals = np.nansum(agg_ts,axis=0)
            series_medians = np.nanmedian(agg_ts,axis=1)
            #exclude max point for mean/std calculation
            series_top = np.nanmax(agg_ts,axis=1)
            mask = np.equal(agg_ts,series_top[:,None])
            to_check = agg_ts.copy()
            to_check[mask] = np.nan
            series_means = np.nanmean(to_check,axis=1)
            series_std = np.nanstd(to_check,axis=1)
            series_max = sigcut*series_std+series_means
            daily_max = np.nanmax(agg_ts,axis=0) * cutoff
            horiz_spike = np.greater(agg_ts,series_max[:,None])
            vert_spike = np.greater(agg_ts,daily_max[None,:])
            hard_cut = agg_ts > hardcut
            total_spike = np.logical_and(horiz_spike,np.logical_and(vert_spike,hard_cut))
            spike_index = np.nonzero(total_spike)
            si_pairs = zip(list(spike_index[0]),list(spike_index[1]))
            for ri,ci in si_pairs: #one at a time?  Slow!  Fix!!
                #convert sub array row index back to master index
                pindex = group_pages[ri]
                master_index = tsa.p2r[pindex]
                outlier_subs[master_index,ci] = series_medians[ri]
                #print "   clipping series %6g on %11s from %7g to %7g" % (plist[ri],date_str[ci],agg_ts[ri,ci],daily_max[ci])
        print "   flagged %g out of %g points as outliers" % (np.count_nonzero(outlier_subs),outlier_subs.shape[0]*outlier_subs.shape[1])
        np.seterr(all='warn')
        return outlier_subs
    
    def agg_series(self,agg_scheme,tsa,source_array,w_scheme=None,norm=None):
        """
        """
        print "Performing Series Aggregation"
        raw_data = tsa.arrays[source_array]
        agg_data = {}
        if w_scheme is not None:
            if w_scheme == "inv":
                weights = agg_scheme.weight_matrix_simple.copy()
            elif w_scheme == "cos":
                weights = agg_scheme.weight_matrix.copy()
            else:
                print "Invalid weight scheme!",w_scheme
                sys.exit()
            reindex = list(agg_scheme.w_p2r[tsa.r2p[i]] for i in range(raw_data.shape[0]))
            weights = weights[reindex] #reindex to be consistent
        else:
            weights = np.ones(agg_scheme.weight_matrix.shape)
        if norm is not None:
            master_totals={}
        for gdict in agg_scheme.scheme["groups"]:
            gname = gdict["group_name"]
            group_pages = gdict["group_pages"]
            group_master = gdict["group_master"]
            gindex = list(tsa.p2r[pindex] for pindex in group_pages if pindex in tsa.p2r)
            wc = agg_scheme.w_g2c[gname]
            pweights = np.array(list(weights[gi,wc] for gi in gindex))
            agg_ts = np.multiply(raw_data[gindex,:],pweights[:,None])
            daily_totals = np.nansum(agg_ts,axis=0)
            if norm is not None and group_master != "MASTER":
                cur_tot = master_totals.get(group_master,np.zeros(daily_totals.shape[0]))
                cur_tot = cur_tot + daily_totals
                master_totals[group_master] = cur_tot
            agg_data[gname] = (tsa.dt_list,list(daily_totals))
        if norm is None:
            return agg_data
        elif norm == "all":
            grand_total = np.zeros(tsa.n_days)
            for mt in master_totals.values():
                grand_total = grand_total+mt
            for gdict in agg_scheme.scheme["groups"]:
                gname = gdict["group_name"]
                dvt = agg_data[gname]
                output = np.divide(np.array(dvt[1]),grand_total)*100.0
                agg_data[gname] = (dvt[0],list(output))
            return agg_data
        elif norm == "bygroup":
            for gdict in agg_scheme.scheme["groups"]:
                group_name = gdict["group_name"]
                group_master=gdict["group_master"]
                if group_master=="MASTER":
                    continue
                group_total=master_totals[group_master]
                group_dv = agg_data[group_name]
                output = np.divide(np.array(group_dv[1]),group_total)*100.0
                agg_data[group_name] = (group_dv[0],list(output))
            return agg_data
        else:
            print "Arg norm=%s not understood, raw data output" % norm
            return agg_data


    def agg_by_month(self,tsa,source_array,agg_type=None):
        """
        For the moment, try pandas grouping feature, return list of str(Month year) and list of data

        """
        dt_list = tsa.dt_list
        pd_dates = pd.to_datetime(dt_list)
        pdf = pd.DataFrame(pd_dates, columns=['date'])
        data = tsa.arrays[source_array]
        agg_data={}
        col_names = []
        for ri,row in enumerate(data):
            name = tsa.r2p[ri]
            col_names.append(name)
            pdf[name]=row
        if agg_type == "mean":
            by_month = pdf.groupby(pd.Grouper(key='date',freq='M')).mean()
        elif agg_type == "median":
            by_month = pdf.groupby(pd.Grouper(key='date',freq='M')).median()
        else:
            by_month = pdf.groupby(pd.Grouper(key='date',freq='M')).sum()
        #by_month.index = by_month.index.strftime('%b-%Y')
        mo_dt_list = list(dti.to_pydatetime() for dti in by_month.index)
        for col in col_names:
            agg_data[col] = (mo_dt_list,list(by_month[col]))
        return agg_data
                            

    def get_searchterm_counts(self,agg_scheme,db_session,target="clicks"):
        """
        for all terms in an aggregation scheme, tally and weight hits from search queries
        by day, convert to time_series, returns a dict of gterm-->[dates,values]
        """
        sql = "SELECT DISTINCT date FROM searchdata"
        dres = db_session.session.execute(sql).fetchall()
        dt_list = list(t[0] for t in dres)
        dt_list.sort()
        search_counts = {}
        ugroup_names = set(gdict["group_name"].lower() for gdict in agg_scheme.scheme["groups"])
        print "Fetching Search Term Data for",target
        for dt in dt_list:
            sql_dt = dt
            sql = 'SELECT sterm,count FROM searchdata WHERE `key`="%s" and date="%s"' % (target,sql_dt)
            results = db_session.session.execute(sql).fetchall()
            sterms = list(tup[0] for tup in results)
            scounts = list(tup[1] for tup in results)
            term_counts = {}
            #scan search string for words matching group names
            #no accounting for variations or misspellings
            for si,sterm in enumerate(sterms):
                terms = set(sterm.split())
                hits = []
                for gterm in ugroup_names:
                    #some terms are composite, must match all words
                    #in group name to search string
                    #e.g. cabernet sauvignon
                    gbreak = gterm.split()
                    num_match = 0
                    for i in range(len(gbreak)):
                        if gbreak[i] in terms:
                            num_match = num_match + 1
                    if num_match >= len(gbreak):
                        hits.append((gterm,scounts[si]))
                if len(hits)>0:
                    n_hits = float(len(hits))
                    for gterm,counts in hits:
                        tc = term_counts.get(gterm,0.0)
                        tc = tc + counts/n_hits #spread clicks across all matched terms
                        term_counts[gterm] = tc
            search_counts[dt] = term_counts
        #search_counts is dictionary by date, convert to timeseries
        search_ts = {}
        for gname in ugroup_names:
            g_counts = list(search_counts[dt].get(gname,0.0) for dt in dt_list)
            search_ts[gname] = [dt_list,g_counts]
        return search_ts

    def calc_scores(self,tsa,source_array,agg_scheme):
        """
        #calculates a "normalized" score for each group in dictionary
        #where each series divided by the sum of all series and multiplied by 100
        #agg_key is what value to aggregate
        #dictionary is updated, no return values
        """
        master_groups = agg_scheme.masters.keys()
        master_wi = (agg_scheme.w_g2c[gn] for gn in master_groups if gn != "MASTER")
        data = tsa.arrays[source_array]
        subscore_dict = {}
        dt_list = tsa.dt_list
        for mg,mc in zip(master_groups,master_wi):
            gmask=agg_scheme.weight_matrix[:,mc] > 0.0
            gindex = list(np.nonzero(gmask)[0])
            gdata = data[gmask,:]
            gsum = np.nansum(gdata,axis=0)
            gnorm = np.divide(gdata,gsum[None,:])*100.0
            for gi in gindex:
                group_name=agg_scheme.c2g[gi]
                group_val = list(gnorm[gi])
                subscore_dict[group_name] = (dt_list,group_val)
        grand_total=np.nansum(data,axis=0)
        grand_norm = np.divide(data,grand_total[None,:])*100.0
        score_dict={}
        for gname,gi in tsa.p2r.iteritems():
            group_val = list(grand_norm[gi])
            score_dict[gname] = (dt_list,group_val)
        return score_dict,subscore_dict

    def calculate_index_scores(self,master_data,agg_scheme,agg_val,agg_dstr,search_val,search_dstr,search_weight=0.5):
        print "CALCULATING scores/subscores for view metric"
        self.calc_scores(master_data,agg_scheme,agg_val,agg_dstr)
        print "CALCULATING scores/subscores for search metric"
        self.calc_scores(master_data,agg_scheme,search_val,search_dstr)
        print "CALCULATING Index values with search weight:",search_weight
        for gname,gdict in master_data.iteritems():
            sdict = gdict["scores"]
            agg_dstr = sdict[agg_val]["score_dstr"]
            agg_score = sdict[agg_val]["score"]
            agg_subscore = sdict[agg_val]["subscore"]
            search_dstr = sdict[search_val]["score_dstr"]
            search_score = sdict[search_val]["score"]
            search_subscore = sdict[search_val]["subscore"]
            common_dates = list(set(agg_dstr) & set(search_dstr))
            only_pv = list(set(agg_dstr) - set(search_dstr))
            only_pv.sort()
            common_dates.sort()
            common_agg_i = list(agg_dstr.index(dstr) for dstr in common_dates)
            common_search_i = list(search_dstr.index(dstr) for dstr in common_dates)
            only_pv_i = list(agg_dstr.index(dstr) for dstr in only_pv)
            index_score_val = []
            index_dstr = []
            index_subscore_val = []
            for di,date in enumerate(common_dates):
                search_i = common_search_i[di]
                pv_i = common_agg_i[di]
                comp_score = search_weight*search_score[search_i] + (1.0 - search_weight)*agg_score[pv_i]
                comp_subscore = search_weight*search_subscore[search_i] + (1.0 - search_weight)*agg_subscore[pv_i]
                index_score_val.append(comp_score)
                index_subscore_val.append(comp_subscore)
                index_dstr.append(date)
            for di,date in enumerate(only_pv):
                pv_i = only_pv_i[di]
                comp_score = agg_score[pv_i]
                comp_subscore = agg_subscore[pv_i]
                index_score_val.append(comp_score)
                index_subscore_val.append(comp_subscore)
                index_dstr.append(date)
            i_scores = zip(index_dstr,index_score_val)
            i_subscores = zip(index_dstr,index_subscore_val)
            i_scores.sort(key = lambda x:x[0])
            i_subscores.sort(key = lambda x:x[0])
            gdict["i_score"] = list(t[1] for t in i_scores)
            gdict["i_subscore"] = list(t[1] for t in i_subscores)
            gdict["score_dstr"] = list(t[0] for t in i_scores)
            
            
