import sys,os,time,datetime,pickle,copy
import numpy as np
#local imports
from vp_data import DataProc,TimeSeries,AggScheme,AggFunc,TsArray
from pv_ingest import Utils
from vp_plot import Plotter
from vp_prop import vinepair_creds

###################################################
#    VINEPAIR SPECIFIC INPUTS
#
# Dates (format is YYYY-mm-dd) - inclusive
# all dates are queried, selected range output
# DATA_ dates are for the range to be analyzed
# EXPORT_ dates are for the range to be plotted/exported
DATA_START_DATE="2017-01-01"
DATA_END_DATE="2019-11-01"
EXPORT_START_DATE="2018-01-01"
EXPORT_END_DATE="2019-11-01"

#
# Paths:
# PATH to CSV for aggregation_scheme import
AGG_CSV_PATH = "new_track.csv"
# Filename Root for JSON output (one file generated for every master category)
JSON_OUTPUT = "test_json"

# What to aggregate for each page?
# Options are "sessions","pageviews","uniquePageviews","avgSessionDuration","entrances","bounceRate","exitRate"
PAGE_CATEGORY = "pageviews"

# Subtract scroll events from the category above (True/False)
SUBTRACT_SEV = True

# Search category to track
# Options are: "clicks","ctr","impressions","position"
SEARCH_TRACK="impressions"
# Weight factor for search category vs page category (float between 0.0 and 1.0)
SEARCH_WEIGHT = 0.5

# Use simple page weighting (inverse of # of tracked tags)
USE_SIMPLE_WEIGHTS=True

# Filtering Cutoffs (all three must be met for a point to be filtered)
# Fraction of day's aggregated total
DAY_CUT = 0.5
# Number of SD above median in entire timeseries to be filtered
SIG_CUT = 10.0
# Absolute count cutoff to be filtered
HARD_CUT = 5000

# Create / Use local copy?
# To speed up processing, save all timeseries arrays and all
# single timeseries queries as pickle files, loaded on startup
# files used/generated are:
#     data_cache.pkl --> dict(key=pindex,val=dict(key=query_column,val=(dates,values)))
#     timeseries_array_XXX.pkl where XXX is PAGE_CATEGORY or "scroll_events"
#
#
USE_LOCAL_COPY = True

# If plotting, give path to a valid chrome browser:
#CHROME_PATH = "/usr/lib64/chromium/chromium"
CHROME_PATH = "/opt/google/chrome/chrome"



#    END OF USER INPUT (AUTOPILOT FROM HERE)
###################################################




#initialize 
util = Utils()
aggfunc = AggFunc()
if os.path.isfile(CHROME_PATH):
    plotter = Plotter(CHROME_PATH)
else:
    print "Improper Chrome Path! Proceed at your own risk!"
    plotter = Plotter(CHROME_PATH)
dt = datetime.datetime
creds = vinepair_creds()
#creds["load_cache"] = USE_LOCAL_COPY
#get processing/DB connection
proc  = DataProc(**creds)
proc.db_init()
db_session = proc.dbsession
db_session.create_lookups()
pterm_lookup = db_session.pterm_lookup
pindex_lookup = db_session.pindex_lookup
pi2slug = db_session.pi2slug


#Initial setup
#all dates in database
all_dt=proc.dt_list #all possible dates from database
all_ndays = len(all_dt)
all_dstr = list(dt.strftime("%Y-%m-%d") for dt in all_dt)

#dates 
start_dt = dt.strptime(DATA_START_DATE,"%Y-%m-%d")
end_dt = dt.strptime(DATA_END_DATE,"%Y-%m-%d")

all_dt = list(dt for dt in all_dt if dt >= start_dt and dt <= end_dt)
all_ndays = len(all_dt)
all_dstr = list(dt.strftime("%Y-%m-%d") for dt in all_dt)


#get aggregation scheme
agg1 = AggScheme()
filen = os.path.basename(AGG_CSV_PATH).split('.')[0].strip().upper()
if len(filen) == 0 or filen is None:
    filen = "Default"
agg1.get_agg_scheme(filen,pterm_lookup,csv_in=AGG_CSV_PATH)
agg1.show()
proc.get_index_matrix()



#group by master cat wine,beer,spirit
master_cats = list(set(list(gdict["group_master"] for gdict in agg1.scheme["groups"])))
group_cat_lookup = {}
cat_group_lookup = {}
for gdict in agg1.scheme["groups"]:
    gname = gdict["group_name"]
    master = gdict["group_master"]
    group_cat_lookup[gname] = master
    cg = cat_group_lookup.get(master,[])
    cg.append(gname)
    cat_group_lookup[master] = cg
    
#calculate page weights
agg1.get_page_weights(proc)

#get pindex for all tracked pages
#all_pages = proc.p2i.keys()
tracked_pages = agg1.get_tracked_pages()


#get all data in tsarray format
#use local data if selected
pv_data = TsArray(tracked_pages,all_dt)
to_update = [PAGE_CATEGORY,SEARCH_TRACK]
if SUBTRACT_SEV:
    to_update.append("scroll_events")
if USE_LOCAL_COPY:
    pv_in = TsArray.load_array("pagedata")
else:
    pv_in = {"arrays":{}}
for update in to_update:
    if update in pv_in.arrays.keys() and set(pv_in.dt_list) == set(pv_data.dt_list):
        existing_pindex = pv_in.p2r.keys()
        pindex_tocopy = list(set(tracked_pages) & set(existing_pindex))
        missing_pindex = list(set(tracked_pages) - set(pindex_tocopy))
        old_array = pv_in.arrays[update]
        target_index = list(pv_data.p2r[pindex] for pindex in pindex_tocopy)
        target_array = pv_data.new_array()
        source_index = list(pv_in.p2r[pindex] for pindex in pindex_tocopy)
        target_array[target_index] = old_array[source_index]
        pv_data.add_array(target_array,update)
        if len(missing_pindex) > 0:
            if update == SEARCH_TRACK:
                toadd_dict = aggfunc.get_searchterm_counts(agg1,db_session,target=SEARCH_TRACK)
            else:
                toadd_dict = proc.get_all_bykey(update,missing_pindex)
            pv_data.insert_by_dict(update,toadd_dict)

    else:
        if update == SEARCH_TRACK:
            toadd_dict = aggfunc.get_searchterm_counts(agg1,db_session,target=SEARCH_TRACK)
        else:
            toadd_dict = proc.get_all_bykey(update,missing_pindex)
        print "No previous data available for",update
        pv_data.insert_by_dict(update,toadd_dict)

    
if SUBTRACT_SEV:
    no_scroll = np.isnan(pv_data.arrays["scroll_events"])
    with_scroll = np.invert(no_scroll)
    net_array = pv_data.arrays[PAGE_CATEGORY].copy()
    net_array[with_scroll] = net_array[with_scroll] - pv_data.arrays["scroll_events"][with_scroll]
    pv_data.add_array(net_array,"net_pv")
else:
    pv_data.arrays["net_pv"] = pv_data.arrays[PAGE_CATEGORY]

#save ts data as local pkl file
if USE_LOCAL_COPY:
    pv_data.store_array("pagedata")

#TSA for aggregated categories
agg_data = TsArray(group_cat_lookup.keys(),all_dt)


#Data accumulation and processing
outlier_subs = aggfunc.flag_outliers(agg1,pv_data,"net_pv",cutoff=0.5,sigcut=10.0,hardcut=5000)
outlier_mask = outlier_subs > 0.01
filt_array = pv_data.arrays["net_pv"].copy()
filt_array[outlier_mask] = outlier_subs[outlier_mask]
pv_data.add_array(outlier_subs,"outliers")
pv_data.add_array(filt_array,"filt_pv")
out_plist=plotter.get_outlier_plot(pv_data,"net_pv","outliers",name_lookup=pi2slug)
agg_dict1 = aggfunc.agg_series(agg1,pv_data,"filt_pv",w_scheme="inv",norm=None)
agg_dict2 = aggfunc.agg_series(agg1,pv_data,"filt_pv",w_scheme="inv",norm="all")
agg_dict3 = aggfunc.agg_series(agg1,pv_data,"filt_pv",w_scheme="inv",norm="bygroup")
agg_dict4 = aggfunc.agg_series(agg1,pv_data,SEARCH_TRACK,w_scheme="inv",norm=None)
pv_agg = TsArray(agg_dict1.keys(),all_dt)
pv_agg.insert_by_dict("raw_agg",agg_dict1)
pv_agg.insert_by_dict("scr_agg",agg_dict2)
pv_agg.insert_by_dict("ssc_agg",agg_dict3)
pv_agg.insert_by_dict("stc_agg",agg_dict4)
mo_dict1=aggfunc.agg_by_month(pv_agg,"raw_agg",agg_type="median")
mo_dict2=aggfunc.agg_by_month(pv_agg,"scr_agg",agg_type="median")
mo_dict3=aggfunc.agg_by_month(pv_agg,"ssc_agg",agg_type="median")
mo_dt_list = aggfunc.truncate_dt2mo(pv_agg)
mo_agg = TsArray(mo_dict1.keys(),mo_dt_list)
#mo_agg.insert_by_dict("raw_mo",mo_dict1)
mo_agg.insert_by_dict("scr_mo",mo_dict2)
mo_agg.insert_by_dict("ssc_mo",mo_dict3)
pv_data.show()
pv_agg.show()
mo_agg.show()



panel_dlist = []

for series in pv_agg.p2r.keys():
    to_plot = list((frame,series) for frame in [SEARCH_TRACK,])#pv_agg.arrays.keys())
    plot_dict = plotter.tsarray_to_plot(pv_agg,to_plot,str(series),start=EXPORT_START_DATE,end=EXPORT_END_DATE)
    panel_dlist.append(plot_dict)
fig1 = plotter.make_plot(panel_dlist,master_title="Aggregated Series")
fig1.show()
"""
panel_dlist = []

for series in mo_agg.p2r.keys():
    to_plot = list((frame,series) for frame in mo_agg.arrays.keys())
    plot_dict = plotter.tsarray_to_plot(mo_agg,to_plot,str(series))
    panel_dlist.append(plot_dict)
fig2 = plotter.make_plot(panel_dlist,master_title="Aggregation by Month")
fig2.show()
"""
for ci,cat in enumerate(master_cats):
    filename = JSON_OUTPUT+"_"+cat+"_scores.json"
    util.ts2json(mo_agg,"scr_mo",output_file=filename,start=EXPORT_START_DATE,end=EXPORT_END_DATE)
    filename = JSON_OUTPUT+"_"+cat+"_subscores.json"
    util.ts2json(mo_agg,"ssc_mo",output_file=filename,start=EXPORT_START_DATE,end=EXPORT_END_DATE)


sys.exit()





