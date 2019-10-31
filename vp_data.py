import sys,os,time,datetime,pickle,csv
from statsmodels.tsa.seasonal import seasonal_decompose
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import Column, Integer, Float,String, DateTime,MetaData,Table
from sqlalchemy.ext.declarative import declarative_base
from pv_database import DBsession
import pandas as pd

tracked_types_csv_file="tracked_types_cats.csv"

class DataProc():
    def __init__(self,mysql_login,mysql_pass,mysql_host,mysql_port,database):
        #assume initialized database for now
        self.dbsession = DBsession(mysql_login,mysql_pass,mysql_host,mysql_port,database)
        self.dbsession.index_columns()
        self.dbe = self.dbsession.session.execute
        self.index_mat = None
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
        """
        Fetches time series for a given pindex and key (e.g. pageviews)
        Updates a cache for repeat queries

        """
        cur_pindex_data = self.data_cache.get(pindex,{})
        if key in cur_pindex_data.keys():
            print "Fetching %g %s timeseries from cache" % (pindex,key)
            return cur_pindex_data[key]
        else:
            sql_in = 'SELECT date,SUM(count) AS total FROM pagedata WHERE pindex=%s AND `key`="%s" GROUP BY date' % (pindex,key)
            print "Fetching %g %s timeseries from DATABASE" % (pindex,key)
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
    #no longer used    
    #def get_sumseries_plist(self,plist,key):
    #    plist_sql = "("+",".join(list("%d" % pindex for pindex in plist))+")"
    #    sql_in = 'SELECT date,SUM(count) AS total FROM pagedata WHERE pindex IN %s AND `key`="%s" GROUP BY date' % (plist_sql,key)
    #    query = self.dbe(sql_in)
    #    dates,values = [],[]
    #
    #    for result in query:
    #        dates.append(result[0])
    #        values.append(result[1])
    #    return values,dates

    

    def aggregate_by_plist(self,plist,key,prefilter=False):
        n_rows = len(plist)
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
            tindex_list = list(set(tindex_list + tlist))
        tindex_list.sort()
        n_rows = len(pindex_list)
        n_col = len(tindex_list)
        imatrix = np.zeros((n_rows,n_col),dtype=np.bool_)
        p2i = {}
        i2p = {}
        t2i = {}
        i2t = {}
        for i,pindex in enumerate(pindex_list):
            p2i[pindex] = i
            i2p[i] = pindex
        for i,tindex in enumerate(tindex_list):
            t2i[tindex] = i
            i2t[i] = tindex
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
             key=group_name, e.g. Whiskey
             val = list of associated pindex

        """
        rev_look = {}
        track_lookup={}
        with open(csv_in,'r') as track_f:
            reader = csv.reader(track_f,delimiter=',')
            header = next(reader)
            rows = [(row[0], list(int(row[i]) for i in range(1,len(row)) if len(row[i]) > 0)) for row in reader]
        track_lookup = {}
        for track_t in rows:
            track_name = track_t[0]
            tindex_include = list(tindex for tindex in track_t[1] if tindex > 0)
            tindex_exclude = list(-tindex for tindex in track_t[1] if tindex < 0)
            track_lookup[track_name] = {"include":tindex_include,"exclude":tindex_exclude}
        tracked_tindex = []
        for tdict in track_lookup.values():
            for tindex in tdict["include"]:
                tracked_tindex.append(tindex)
        self.tracked_tindex = tracked_tindex
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
                gdict = {"group_name":group_name,"group_pages":final_pages}
                self.scheme["groups"].append(gdict)
            else:
                existing_groups = list(gdict["group_name"] for gdict in self.scheme["groups"])
                if group_name in existing_groups:
                    for gdict in self.scheme["groups"]:
                        if gdict["group_name"] == group_name:
                            gdict["group_pages"] = list(set(gdict["group_pages"] + final_pages))
                else:
                    gdict = {"group_name":group_name,"group_pages":final_pages}
                    self.scheme["groups"].append(gdict) 
                    

    def show(self):
        scheme_name = self.scheme["name"]
        n_groups = len(self.scheme["groups"])
        print "Scheme %s contains %d groups:" % (scheme_name,n_groups)
        for gn,gdict in enumerate(self.scheme["groups"]):
            group_name = gdict["group_name"]
            group_pages = gdict["group_pages"]
            n_pages = len(group_pages)
            print "    Group: %20s  contains %5d pages." % (group_name,n_pages) 
                       
    def get_page_weights(self,proc):
        """
        PROPRIATARY CODE, REMOVED FROM PUBLIC REPOSITORY
        """

class AggFunc():
    """
    Collection of functions for aggregating, weighting, and filtering TimeSeries objects
    """
    def __init__(self):
        pass





    
    def agg_remove_spikes(self,proc,plist,metric,cutoff=0.5,sigcut=10.0,hardcut=5000,remove_scroll=True):
        """
        PROPRIATARY CODE, REMOVED FROM PUBLIC REPOSITORY
        """
    def ts_to_array(self,ts_list):
        # takes a list of timeseries and creates a numpy structured array
        # allows for missing data, shifted series, etc.
        # returns np array and a list of dates for each column
        dates_list = []
        for ts in ts_list:
            dates_list = dates_list + ts.dates
        dates_list = list(set(dates_list))
        dates_list.sort()
        date_lookup = {}
        for di,dt in enumerate(dates_list):
            date_lookup[dt] = di
        dates_str = list(dt.strftime("%Y-%m-%d") for dt in dates_list)
        n_col = len(dates_list)
        n_row = len(ts_list)
        arr = np.ones((n_row,n_col),dtype=np.float64) * np.nan
        for ti,ts in enumerate(ts_list):
            d = ts.dates
            v = ts.data
            for di,dt in enumerate(d):
                coli = date_lookup[dt]
                arr[ti,coli] = v[di]
        return arr,dates_list



    def tsa_arith(self,dates1, vals1, dates2, vals2,opp="subtract"):
        """  
        Takes two timeseries as lists, one dates, other values.
        Arithmetic is "add, subtract, multiply, divide" a new list of values for list 1 returned
        Each date/vals pair must be aligned (same length).  If values are missing from the 2nd date/val
        pair, the original value is returned unchanged
        
        """
        if opp not in ["add","subtract","multiply","divide"]:
            print 'ERROR, operator not recognized as one of "add","subtract","multiply","divide"'
            return list(x for x in vals1)
        if len(dates1) != len(vals1):
            print "ERROR, incorrect input format for dates,values!"
            return list(x for x in vals1)
        v2lookup = {}
        for d2index,d2date in enumerate(dates2):
            v2lookup[d2date] = vals2[d2index]
        #fill in list with appropriate null values
        if opp == "subtract" or opp == "add":
            null_val = 0.0
        elif opp == "multiply" or opp == "divide":
            null_val == 1.0
        #what are we adding/subtracting/etc?    
        left_val = list(v2lookup.get(date,null_val) for date in dates1)
        if opp == 'add':
            out_val = list(vals1[i] + left_val[i] for i in range(len(vals1)))
        if opp == 'subtract':
            out_val = list(vals1[i] - left_val[i] for i in range(len(vals1)))
        if opp == 'multiply':
            out_val = list(vals1[i] * left_val[i] for i in range(len(vals1)))
        if opp == 'divide':
            #quick check for zero division
            out_val = list(vals1[i] / left_val[i] if left_val[i] != 0.0 else vals1[i] for i in range(len(vals1)))
        return out_val

    
    def weight_pages(self,ts_array,agg_scheme,target):
        """
        Input:
           ts_array = numpy array of timeseries (row=page, col=date) 
           agg_scheme = AggScheme instance with page weights
           target is "group_name" from AggScheme
        """
        for gdict in agg_scheme.scheme["groups"]:
            if gdict["group_name"] == target:
                arr_copy = ts_array.copy()
                weights = gdict.get("weights",None)
                if weights is None:
                    print "ERROR, no page weights in place, call get_page_weights() first!"
                    return ts_array
                np_weights = np.array(weights)
                assert np_weights.shape[0] == arr_copy.shape[0]
                scaled_array = arr_copy*np_weights[:,None]
                return scaled_array
        #if not group not found    
        print "ERROR, target %s not in aggregation scheme!" % target
        return ts_array

    def agg_by_month(self,values,dt_list):
        """
        For the moment, try pandas grouping feature, return list of str(Month year) and list of data

        """
        pd_dates = pd.to_datetime(dt_list)
        pdf = pd.DataFrame(pd_dates, columns=['date'])
        pdf["data"] = values
        by_month = pdf.groupby(pd.Grouper(key='date',freq='M')).sum()
        by_month.index = by_month.index.strftime('%b-%Y')
        return list(by_month.index),list(by_month.data)

    def arr_show(self,arr,dates_list):
        dates_str = list(dt.strftime("%Y-%m-%d") for dt in dates_list)
        if len(dates_str) > 5:
            print "TS ARRAY     %12s %12s %12s . . . %12s %12s %12s" % (dates_str[0],dates_str[1],dates_str[2],
                                                                        dates_str[-3],dates_str[-2],dates_str[-1])
            for row in arr:
                print "    series   %12.2f %12.2f %12.2f . . . %12.2f %12.2f %12.2f" % (row[0],row[1],row[2],
                                                                                      row[-3],row[-2],row[-2])
        else:
            print dates_str
            print arr


    def get_searchterm_counts(self,agg_scheme,dt_list,db_session,target="clicks"):
        """
        for all terms in an aggregation scheme, tally and weight hits from search queries
        by day, convert to time_series, returns a dict of gterm-->[dates,values]
        """
        search_counts = {}
        ugroup_names = set(gdict["group_name"].lower() for gdict in agg_scheme.scheme["groups"])
        for dt in dt_list:
            sql_dt = dt.strftime("%Y-%m-%d %H-%M-%S")
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
                    if gterm in terms:
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

