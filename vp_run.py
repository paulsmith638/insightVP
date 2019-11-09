import sys,os,time,datetime,pickle,copy
import numpy as np
#local imports
from vp_data import DataProc,TimeSeries,AggScheme,AggFunc,TsArray
from pv_ingest import Ingest
from pv_ingest import Utils
from vp_plot import Plotter
from vp_prop import vinepair_creds

###################################################
#    VINEPAIR SPECIFIC INPUTS
#
# Dates (format is YYYY-mm-dd) - inclusive
# all dates are queried, selected range output
# SINGLE_WINDOW = True/False, use the defined date range
#   for all analysis and output or use all available data
#   for analysis/weighting and only output the define range
START_DATE="2017-01-01"
END_DATE="2019-06-30"
SINGLE_WINDOW = True
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
CHROME_PATH = "/usr/lib64/chromium/chromium"

#    END OF USER INPUT (AUTOPILOT FROM HERE)
###################################################




#initialize 
util = Utils()
#ingest = Ingest()
aggfunc = AggFunc()
if os.path.isfile(CHROME_PATH):
    plotter = Plotter(CHROME_PATH)
else:
    print "Improper Chrome Path! Plotting disabled!"
    plotter = None
dt = datetime.datetime
creds = vinepair_creds()
creds["load_cache"] = USE_LOCAL_COPY
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
start_dt = dt.strptime(START_DATE,"%Y-%m-%d")
end_dt = dt.strptime(END_DATE,"%Y-%m-%d")

window_dt = list(dt for dt in all_dt if dt >= start_dt and dt <= end_dt)
window_ndays = len(window_dt)
window_dstr = list(dt.strftime("%Y-%m-%d") for dt in window_dt)

if SINGLE_WINDOW:
    all_dt = window_dt
    all_ndays = window_ndays
    all_dstr = window_dstr

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
    
#get search_counts for all agg_scheme groups
if "search_counts" in proc.data_cache.keys() and USE_LOCAL_COPY:
    print "Loading search term data from cache"
    search_counts = proc.data_cache["search_counts"]
else:
    search_counts = aggfunc.get_searchterm_counts(agg1,db_session,target=SEARCH_TRACK)

if USE_LOCAL_COPY:
    proc.data_cache["search_counts"] = search_counts
    print "Saving data cache"
    f = open("data_cache.pkl",'wb')
    pickle.dump(proc.data_cache,f)
    f.close()
    
#calculate page weights
agg1.get_page_weights(proc)

#get pindex for all tracked pages
#all_pages = proc.p2i.keys()
tracked_pages = agg1.get_tracked_pages()


#get all data in tsarray format
#use local data if selected
pv_data = TsArray(tracked_pages,all_dt)
to_update = [PAGE_CATEGORY,]
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
            toadd_dict = proc.get_all_bykey(update,missing_pindex)
            pv_data.insert_by_dict(update,toadd_dict)

    else:
        print "No previous data available for",update
        toadd_dict = proc.get_all_bykey(update,tracked_pages)
        pv_data.insert_by_dict(update,toadd_dict)

    
if SUBTRACT_SEV:
    no_scroll = np.isnan(pv_data.arrays["scroll_events"])
    with_scroll = np.invert(no_scroll)
    net_array = pv_data.arrays[PAGE_CATEGORY].copy()
    net_array[with_scroll] = net_array[with_scroll] - pv_data.arrays["scroll_events"][with_scroll]
    pv_data.add_array(net_array,"net_pv")
else:
    pv_data.arrays["net_pv"] = pv_data.arrays[PAGE_CATEGORY]




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
agg_dict1 = aggfunc.agg_series(agg1,pv_data,"filt_pv",w_scheme="inv",norm=None)
agg_dict2 = aggfunc.agg_series(agg1,pv_data,"filt_pv",w_scheme="inv",norm="all")
agg_dict3 = aggfunc.agg_series(agg1,pv_data,"filt_pv",w_scheme="inv",norm="bygroup")
pv_agg = TsArray(agg_dict1.keys(),all_dt)
#for ti,tdict in enumerate([agg_dict1,agg_dict2,agg_dict3]):
#    for k,v in tdict.iteritems():
#        print ti,k,v
pv_agg.insert_by_dict("raw_agg",agg_dict1)
pv_agg.insert_by_dict("scr_agg",agg_dict2)
pv_agg.insert_by_dict("ssc_agg",agg_dict3)
mo_dict1=aggfunc.agg_by_month(pv_agg,"raw_agg",agg_type="median")
mo_dict2=aggfunc.agg_by_month(pv_agg,"scr_agg",agg_type="median")
mo_dict3=aggfunc.agg_by_month(pv_agg,"ssc_agg",agg_type="median")
for k,v in mo_dict1.iteritems():
    mo_dt_list = v[0]
    break
mo_dt_list.sort()
mo_agg = TsArray(mo_dict1.keys(),mo_dt_list)
#mo_agg.insert_by_dict("raw_mo",mo_dict1)
mo_agg.insert_by_dict("scr_mo",mo_dict2)
mo_agg.insert_by_dict("ssc_mo",mo_dict3)
pv_data.show()
pv_agg.show()
mo_agg.show()



panel_dlist = []

top_10i = np.argsort(np.nansum(pv_data.arrays["scroll_events"],axis=1))[::-1][0:10]
series_plot = list(pv_data.r2p[i] for i in top_10i)
view_plot = pv_data.arrays.keys()
#for series in series_plot:
#    to_plot = list((frame,series) for frame in view_plot)
#    plot_dict = plotter.tsarray_to_plot(pv_data,to_plot,str(series)+" : "+pi2slug.get(series,"Unk"))
#    panel_dlist.append(plot_dict)
for series in mo_agg.p2r.keys():
    to_plot = list((frame,series) for frame in mo_agg.arrays.keys())
    plot_dict = plotter.tsarray_to_plot(mo_agg,to_plot,str(series))
    panel_dlist.append(plot_dict)
fig1 = plotter.make_plot(panel_dlist,master_title="HOPE for the best!")
fig1.show()
sys.exit()

for gdict in agg1.scheme["groups"]:
    gname = gdict["group_name"]
    gmaster = gdict["group_master"]
    group_names.append(gname)
    group_pages = gdict["group_pages"]
    page_iweights = gdict["inv_weights"]
    page_cweights = gdict["cos_weights"]
    #raw data, unfiltered, no scroll events removed
    gindex = list(pv_data.p2r[pindex] for pindex in group_pages)
    raw_agg = np.nansum(pv_data.array[gindex,:],axis=0)
    #aggregated data, filtered and scroll_events subtracted, returns numpy array of filtered values
    agg_ts,agg_dates = aggfunc.agg_remove_spikes(proc,group_pages,PAGE_CATEGORY,cutoff=DAY_CUT,sigcut=SIG_CUT,
                                                 hardcut=HARD_CUT)
    #apply page weights to numpy array
    w_ts = aggfunc.weight_pages(agg_ts,agg1,gname,weights="inv")
    #actual aggregation, filtered but unweighted
    agg_sum = list(np.nansum(agg_ts,axis=0))
    #track scroll events
    ev_agg_val,ev_agg_dates = proc.aggregate_by_plist(group_pages,"scroll_events")
    ev_agg_dstr = list(dt.strftime("%Y-%m-%d") for dt in ev_agg_dates)
    #weighted aggregation
    w_sum = list(np.nansum(w_ts,axis=0))
    #date strings for weighted and unweighted totals
    agg_date_str =  list(dt.strftime("%Y-%m-%d") for dt in agg_dates)
    #aggregate by month
    month_dates,month_agg = aggfunc.agg_by_month(w_sum,agg_dates)
    monthly_lists.append(month_agg)
    #get search term totals
    search_dates,search_val = search_counts.get(gname.lower().strip(),([],[]))
    search_dstr = list(dt.strftime("%Y-%m-%d") for dt in search_dates)
    master_data[gname] = {"group_master":gmaster,
                          "raw_agg":raw_agg,"raw_dstr":raw_agg_dstr,
                          "filt_agg":agg_sum,"agg_dstr":agg_date_str,
                          "w_agg":w_sum,
                          "mo_dates":month_dates,"mo_agg":month_agg,
                          "ev_agg_dstr":ev_agg_dstr,"ev_agg_val":ev_agg_val,
                          "search_dstr":search_dstr,"search_val":search_val}


aggfunc.calculate_index_scores(master_data,agg1,"w_agg","agg_dstr","search_val","search_dstr",search_weight=SEARCH_WEIGHT)
    
# for debugging, save master_data for quick reload
#f = open("master_data.pkl",'wb')
#pickle.dump(master_data,f)
#f.close()

#aggregate by month here?
for gname,gdict in master_data.iteritems():
    dt_list = list(dt.strptime(date,"%Y-%m-%d") for date in gdict["score_dstr"])
    month_dstr,month_score = aggfunc.agg_by_month(gdict["i_score"],dt_list)
    month_dstr,month_subscore = aggfunc.agg_by_month(gdict["i_subscore"],dt_list)
    gdict["mo_i_dstr"] = month_dstr
    gdict["mo_i_score"] = month_score
    gdict["mo_i_subscore"] = month_subscore

#generate plots    
panel_dlist = []
for gname,gdict in master_data.iteritems():
    master = gdict["group_master"]
    #if master.upper() != "WINE":
    #    continue
    pdict = {}
    pdict["name"] = gname
    slist = []
    sdict1 = {"name":gname+"_score","dstr":gdict["score_dstr"],"val":gdict["i_score"]}
    sdict2 = {"name":gname+"_subscore","dstr":gdict["score_dstr"],"val":gdict["i_subscore"]}
    slist.append(sdict1)
    slist.append(sdict2)
    pdict["series"] = slist
    panel_dlist.append(pdict)

fig = plotter.make_plot(panel_dlist,master_title="SCORES/SUBSCORES for WINE by Cat")
fig.show(renderer="chrome")

panel_dlist = []
for ci,cat in enumerate(master_cats):
    panel_score_slist = []
    panel_subscore_slist = []
    mo_dates = []
    mo_scores = []
    mo_subscores = []
    cat_gnames = []
    for gname,gdict in master_data.iteritems():
        if gdict["group_master"] == cat:
            sdict1 = {"name":gname+"_score","dstr":gdict["mo_i_dstr"],"val":gdict["mo_i_score"]}
            sdict2 = {"name":gname+"_subscore","dstr":gdict["mo_i_dstr"],"val":gdict["mo_i_subscore"]}
            panel_score_slist.append(sdict1)
            panel_subscore_slist.append(sdict2)
            mo_dates.append(gdict["mo_i_dstr"])
            mo_scores.append(gdict["mo_i_score"])
            mo_subscores.append(gdict["mo_i_subscore"])
            cat_gnames.append(gname)
    pdict1 = {}
    pdict1["name"] = cat+"_scores"
    pdict2 = {}
    pdict2["name"] = cat+"_subscores"
    pdict1["series"] = panel_score_slist
    pdict2["series"] = panel_subscore_slist
    panel_dlist.append(pdict1)
    panel_dlist.append(pdict2)
    filename = JSON_OUTPUT+"_"+cat+"_scores.json"
    util.ts2json(mo_scores,mo_dates,cat_gnames,output_file=filename)
    filename = JSON_OUTPUT+"_"+cat+"_subscores.json"
    util.ts2json(mo_subscores,mo_dates,cat_gnames,output_file=filename)

fig1 = plotter.make_plot(panel_dlist,master_title="SCORES/SUBSCORES by Cat")
fig1.show(renderer="chrome")
sys.exit()

for ci,cat in enumerate(master_cats):
    month_vals = None
    filename = JSON_OUTPUT+"_"+cat+"_scores.json"
    util.ts2json(month_vals,month_dates,group_names,output_file=filename)
    filename = JSON_OUTPUT+"_"+cat+"_subscores.json"
    util.ts2json(imo_vals,imo_dates,group_names,output_file=filename)




sys.exit()





