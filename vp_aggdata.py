import sys,os,time,datetime,pickle,copy
import numpy as np
#local imports
from vp_data import DataProc,AggScheme,AggFunc,TsArray
from pv_ingest import Utils
from vp_plot import Plotter
from vp_prop import vinepair_creds

###################################################
#    VINEPAIR SPECIFIC INPUTS
#
# Dates (format is YYYY-mm-dd) - inclusive
# all dates are queried, selected range output
# DATA_ dates are for the range to be analyzed
# EXPORT_ dates are for the range to be exported
DATA_START_DATE="2016-01-01"
DATA_END_DATE="2019-11-13"
EXPORT_START_DATE="2017-01-01"
EXPORT_END_DATE="2019-11-13"

# AGGREGATION SCHEME
# AGG_CSV_PATH = PATH to CSV for aggregation_scheme import
# AGG_TYPE = how to aggregate for each month, "median","mean", any other is interpreted as "sum"
# WEIGHT_SCHEME = how to weight pages in each group, inv=inverse page frequency, cos = cosine similarity
#     None or any other value means no weighting (all views counted in all groups)
AGG_CSV_PATH = "new_track.csv"
AGG_MODE = "median"
WEIGHT_SCHEME = "inv"

# JSON OUTPUT CONTROL
# JSON_OUTPUT = Filename Root for JSON output (one file generated for every master category)
# JSON_CAPS = words that should be in ALL CAPS in JSON output (case insensitive here)
JSON_OUTPUT = "test_json"
JSON_CAPS = ["ipa"]

# PAGE AGGREGATION SETTINGS
# PAGE_CATEGORY = What to aggregate for each page? Options are "sessions","pageviews","uniquePageviews",
#         "avgSessionDuration","entrances","bounceRate","exitRate"
# SUBTRACT_SEV = subtract scroll events from page_category?  True/False

PAGE_CATEGORY = "pageviews"
SUBTRACT_SEV = True

# SEARCH TERM INCORPORATION
# SEARCH_TRACK = What to count?  Options are: "clicks","ctr","impressions","position"
# SEARCH_WEIGHT = how much to weight search vs page counts? (float 0.0 to 1.0 inclusive)
SEARCH_TRACK="impressions"
SEARCH_WEIGHT = 0.5

# OUTLIER FILTERING
# Filtering Cutoffs (all three must be met for a point to be filtered)
# Filtered values are set to DAY_CUT
#   DAY_CUT = fraction of day's aggregated total
#   SIG_CUT = #SD above median in entire timeseries
#   HARD_CUT = Absolute cutoff in counts
DAY_CUT = 0.5
SIG_CUT = 10.0
HARD_CUT = 5000

# Create / Use local copy?
# To speed up processing, save all timeseries arrays objects and reload on start
#     timeseries_array_XXX.pkl where XXX is PAGE_CATEGORY, search, agg, etc.
USE_LOCAL_COPY = True

#    END OF USER INPUT (AUTOPILOT FROM HERE)
###################################################

#initialize 
util = Utils()
aggfunc = AggFunc()
dt = datetime.datetime
creds = vinepair_creds()

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

#dates 
start_dt = dt.strptime(DATA_START_DATE,"%Y-%m-%d")
end_dt = dt.strptime(DATA_END_DATE,"%Y-%m-%d")
all_dt = list(dt for dt in all_dt if dt >= start_dt and dt <= end_dt)

#get aggregation scheme
agg1 = AggScheme()
filen = os.path.basename(AGG_CSV_PATH).split('.')[0].strip().upper()
if len(filen) == 0 or filen is None:
    filen = "Default"
agg1.get_agg_scheme(filen,pterm_lookup,csv_in=AGG_CSV_PATH)
agg1.show()
proc.get_index_matrix() # needed for fast indexing

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
tracked_pages = agg1.get_tracked_pages()


#get all data in tsarray format
#use local data if selected
pv_data = TsArray(tracked_pages,all_dt)
to_update = [PAGE_CATEGORY,]
if SUBTRACT_SEV:
    to_update.append("scroll_events")
for update in to_update:
    aggfunc.update_tsa(proc,pv_data,update,local=USE_LOCAL_COPY,file="pagedata")

if SUBTRACT_SEV:
    no_scroll = np.isnan(pv_data.arrays["scroll_events"])
    with_scroll = np.invert(no_scroll)
    net_array = pv_data.arrays[PAGE_CATEGORY].copy()
    net_array[with_scroll] = net_array[with_scroll] - pv_data.arrays["scroll_events"][with_scroll]
    pv_data.add_array(net_array,"net_pv")
else:
    pv_data.arrays["net_pv"] = pv_data.arrays[PAGE_CATEGORY]

#save ts data as local pkl file
pv_data.store_array("pagedata")

#   Data accumulation and processing
#


#TSA for aggregated categories
pv_agg = TsArray(group_cat_lookup.keys(),all_dt)
self_agg = agg1.get_selfagg()

#search terms
aggfunc.get_search_data(db_session,agg1,pv_agg,SEARCH_TRACK,local=USE_LOCAL_COPY,file="agg")

#filter outliers
outlier_subs = aggfunc.flag_outliers(agg1,pv_data,"net_pv",cutoff=DAY_CUT,sigcut=SIG_CUT,hardcut=HARD_CUT)
outlier_mask = outlier_subs > 0.01
filt_array = pv_data.arrays["net_pv"].copy()
filt_array[outlier_mask] = outlier_subs[outlier_mask]
pv_data.add_array(outlier_subs,"outliers")
pv_data.add_array(filt_array,"filt_pv")

#aggregation by tracked_terms
agg_dict1 = aggfunc.agg_series(agg1,pv_data,"filt_pv",w_scheme=WEIGHT_SCHEME,norm=None)
agg_dict2 = aggfunc.agg_series(agg1,pv_data,"filt_pv",w_scheme=WEIGHT_SCHEME,norm="all")
agg_dict3 = aggfunc.agg_series(agg1,pv_data,"filt_pv",w_scheme=WEIGHT_SCHEME,norm="bygroup")
agg_dict4 = aggfunc.agg_series(self_agg,pv_agg,SEARCH_TRACK,w_scheme=None,norm="all")
agg_dict5 = aggfunc.agg_series(self_agg,pv_agg,SEARCH_TRACK,w_scheme=None,norm="bygroup")
pv_agg.insert_by_dict("raw_agg",agg_dict1)
pv_agg.insert_by_dict("score_agg",agg_dict2)
pv_agg.insert_by_dict("subscore_agg",agg_dict3)
pv_agg.insert_by_dict("st_score_agg",agg_dict4)
pv_agg.insert_by_dict("st_subscore_agg",agg_dict5)


#combine pageview and search data
search_score = pv_agg.arrays["st_score_agg"]
search_subscore = pv_agg.arrays["st_subscore_agg"]
pv_score = pv_agg.arrays["score_agg"]
pv_subscore = pv_agg.arrays["subscore_agg"]
ssc_nan = np.isnan(search_score)
ssu_nan = np.isnan(search_subscore)
n_rows = ssc_nan.shape[0]
ssc_nanr = np.count_nonzero(ssc_nan,axis=0) == n_rows
ssu_nanr = np.count_nonzero(ssu_nan,axis=0) == n_rows
no_search_col = np.nonzero(np.logical_or(ssc_nanr,ssu_nanr))[0]
with_search_col = np.nonzero(np.invert(np.logical_or(ssc_nanr,ssu_nanr)))[0]
#make composite scores
comp_score = pv_agg.new_array()
comp_subscore = pv_agg.new_array()
comp_score[:,no_search_col] = pv_score[:,no_search_col]
comp_subscore[:,no_search_col] = pv_subscore[:,no_search_col]
comp_score[:,with_search_col] = np.add(pv_score[:,with_search_col]*(1.0 - SEARCH_WEIGHT),
                                       search_score[:,with_search_col]*SEARCH_WEIGHT)
comp_subscore[:,with_search_col] = np.add(pv_subscore[:,with_search_col]*(1.0 - SEARCH_WEIGHT),
                                          search_subscore[:,with_search_col]*SEARCH_WEIGHT)
pv_agg.add_array(comp_score,"score")
pv_agg.add_array(comp_subscore,"subscore")

pv_agg.store_array("agg")

#aggregate by month
mo_dict1=aggfunc.agg_by_month(pv_agg,"score",agg_type=AGG_MODE)
mo_dict2=aggfunc.agg_by_month(pv_agg,"subscore",agg_type=AGG_MODE)
mo_dt_list = aggfunc.truncate_dt2mo(pv_agg)
mo_agg = TsArray(mo_dict1.keys(),mo_dt_list)
mo_agg.insert_by_dict("score_mo",mo_dict1)
mo_agg.insert_by_dict("subscore_mo",mo_dict2)

mo_agg.store_array("agg_mo")
sys.exit()





