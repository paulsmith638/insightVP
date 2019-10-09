import sys,os,time,datetime,pickle
from vp_data import DataProc,TimeSeries,AggScheme
from statsmodels.tsa.seasonal import seasonal_decompose
from pv_ingest import Ingest
from plotly.subplots import make_subplots
import plotly.graph_objects as go
import numpy as np
import pandas as pd
#from pv_database import DBsession
ingest = Ingest()
dt = datetime.datetime
import webbrowser    
urL='https://www.google.com'
#chrome_path="/opt/google/chrome/chrome"
chrome_path="/usr/lib64/chromium/chromium"
webbrowser.register('chrome', None,webbrowser.BackgroundBrowser(chrome_path),1)
webbrowser.get('chrome').open_new_tab(urL)

mysql_login = "psmith"
mysql_pass = "Igrtgd99#"
mysql_host = "54.173.55.254"
mysql_port = 3306
vinepair_database = "vinepair1"

#dbsession = DBsession(mysql_login,mysql_pass,mysql_host,mysql_port,vinepair_database)
#dbsession.create_lookups()

target_start="2018-01-01"
target_end="2019-09-25"

proc  = DataProc(mysql_login,mysql_pass,mysql_host,mysql_port,vinepair_database)
proc.db_init()
db_session = proc.dbsession
db_session.create_lookups()
date_dt=proc.dt_list #all possible dates
n_days = len(date_dt)
date_strings = list(dt.strftime("%Y-%m-%d") for dt in date_dt)
pterm_lookup = db_session.pterm_lookup
agg1 = AggScheme()
agg1.get_agg_scheme("Default",pterm_lookup,csv_in="smith_types.csv")


pindex_lookup = db_session.pindex_lookup
pi2slug = {}
for k,v in pindex_lookup.iteritems():
    pi2slug[v] = k

proc.get_index_matrix()
filter_list = proc.get_plist(1102)
agg1.filter(filter_list)
agg1.show()

sub_titles = ["SPIRIT TRENDS","ARIMA","RESID","SEASONAL","YEAR-over-YEAR"]

whiskey_pages = []
for gdict in agg1.scheme["groups"]:
    if gdict["group_name"] == "WHISKEY":
        whiskey_pages = gdict["group_pages"]
#whiskey_pages = whiskey_pages[0:10]
big_fig = make_subplots(rows=2,cols=1,subplot_titles=sub_titles)

"""
raw_agg_dates,raw_agg_data = proc.get_sumseries_plist(whiskey_pages,"pageviews")
raw_date_str =  list(dt.strftime("%Y-%m-%d") for dt in raw_agg_dates)
filt_dates,filt_agg = proc.aggregate_by_plist(whiskey_pages,"pageviews",prefilter=True)
ts = TimeSeries(filt_dates,filt_agg)
shifted_dates = list((dt - datetime.timedelta(days=365)) for dt in filt_dates)
tr,res,seas = ts.arima_model(filt_dates,filt_agg)
filt_date_str = list(dt.strftime("%Y-%m-%d") for dt in filt_dates)
y2018_dates = list(dt.strftime("%Y-%m-%d") for dt in ts.dates[0:365])
y2018_model = list(x + 3000 for x in tr[0:365])
y2019_data = ts.data[365:]
y2019_dates = list(dt.strftime("%Y-%m-%d") for dt in shifted_dates[365:])
"""
fake_dates = ["2018-01-01","2018-07-01","2019-01-01"]
fake_w = [0.1,0.7,0.6]
fake_t = [0.7,0.2,0.3]
fake_r = [0.2,0.1,0.1]

big_fig.add_trace(go.Scatter(x=fake_dates,y=fake_w,mode='lines',name="WHISKEY"),row=1,col=1)
big_fig.append_trace(go.Scatter(x=fake_dates,y=fake_t,mode='lines',name="ARIMA"),row=1,col=1)
big_fig.append_trace(go.Scatter(x=fake_dates,y=fake_r,mode='lines',name="RUM"),row=1,col=1)

#big_fig.add_trace(go.Scatter(x=raw_date_str,y=raw_agg_data,mode='lines',name="RAW DATA"),row=1,col=1)
#big_fig.append_trace(go.Scatter(x=filt_date_str,y=filt_agg,mode='lines',name="FILT"),row=2,col=1)
#big_fig.append_trace(go.Scatter(x=filt_date_str,y=tr,mode='lines',name="TREND"),row=2,col=1)
#big_fig.append_trace(go.Scatter(x=filt_date_str,y=res,mode='lines',name="RESID"),row=3,col=1)
#big_fig.append_trace(go.Scatter(x=filt_date_str,y=seas,mode='lines',name="SEASONAL"),row=4,col=1)
##big_fig.append_trace(go.Scatter(x=y2018_dates,y=y2018_model,mode='lines',name="2018 Model"),row=5,col=1)
#big_fig.append_trace(go.Scatter(x=filt_date_str[0:365],y=y2018_model,mode='lines',name="2018 Model"),row=5,col=1)
#big_fig.append_trace(go.Scatter(x=y2019_dates,y=y2019_data,mode='lines',name="2019 Data"),row=5,col=1)

big_fig.update_layout(height=1000, width=800, title_text="Subplots")
big_fig.show(renderer="chrome")
sys.exit()
plots = list({'x': px_sort[i],'y':trend_list[i],'type':'lines','name':name_sort[i]+"_T"} for i in range(10))
plots = plot_custom_data("pageviews","Spirits","Default","2018-01-01  00:00:00","2018-12-31  00:00:00",0)
fig = go.Figure()
for plot in plots:
    x = plot['x']
    y = plot['y']
    ptype = plot['type']
    name = plot['name']
    if name[-2] == "_": 
        fig.add_trace(go.Scatter(x=x,y=y,mode=ptype,name=name,line={"width":4}))#,"dash":"dot"}))
    else:
        fig.add_trace(go.Scatter(x=x,y=y,mode=ptype,name=name,line={"width":2}))
fig.show(renderer="chrome")
    
sys.exit()


w_np = np.array(w_data)
for rowi,row in enumerate(w_np):
    date = w_dates[0][rowi].strftime("%Y-%m-%d")
    if np.nanmean(row) > 0:
        rmin = np.amin(row)
        rmax = np.amax(row)
        rave = np.nanmean(row)
        rstd = np.nanstd(row)
        pindex = whiskey_pages[rowi]
        slug = list(key for key,value in db_session.pindex_lookup.iteritems() if value == pindex)[0]
        print "DATA",pindex,slug,rmin,rmax,rave,rstd
        


def plot_custom_data(metric,pgroup,scheme_name,start_date,end_date,n_clicks):
    print metric,pgroup,scheme_name,start_date,end_date,n_clicks
    start_dt = dt.strptime(start_date.split(' ')[0], '%Y-%m-%d')
    end_dt = dt.strptime(end_date.split(' ')[0], '%Y-%m-%d')
    agg_scheme = agg1
    plot_x_list = []
    plot_y_list = []
    series_names = []
    extra_plot_dates = []
    extra_plot_values = []

    for gdict in agg_scheme.scheme['groups']:
        name = gdict['group_name']
        series_names.append(name)
        plist = gdict['group_pages']
        dates,agg_total = proc.get_sumseries_plist(plist,metric)
        if 48218 in plist:
            nd,pv = proc.get_timeseries_pindex(48218,"pageviews")
            for date_i,date_new in enumerate(dates):
                for date_j,date_old in enumerate(nd):
                    if date_new == date_old:
                        new_agg = agg_total[date_i] - pv[date_j]
                        new_dates_str = list(dt.strftime("%Y-%m-%d") for dt in nd)
                        extra_plot_dates.append(new_dates_str)
                        extra_plot_values.append(new_agg)
        dat = zip(dates,agg_total)
        dat.sort(key = lambda x:x[0])
        dates = list(dv[0] for dv in dat)
        agg_total = list(dv[1] for dv in dat)
        dates_str = list(dt.strftime("%Y-%m-%d") for dt in dates)
        plot_x_list.append(dates_str[0:365])
        plot_y_list.append(agg_total[0:365])
    total_pv = []
    for ydat in plot_y_list:
        total_pv.append(np.nansum(ydat))
    to_sort = list(np.argsort(total_pv)[::-1])
    px_sort = list(plot_x_list[i] for i in to_sort)
    py_sort = list(plot_y_list[i] for i in to_sort)
    np_dat = np.array(py_sort)
    daily_totals = np.nansum(np_dat,axis=0)
    normed = []
    for row in np_dat:
        normed.append(list(row/daily_totals))
    name_sort = list(series_names[i] for i in to_sort)
    trend_list = []
    resid_list = []
    seas_list = []
    for di,ydat_in in enumerate(normed):
        ydat = get_median_filtered(ydat_in)
        pdf = pd.DataFrame(ydat)
        pdf.index = pd.to_datetime(px_sort[di])
        result = seasonal_decompose(pdf, model='multiplicative')
        trend =  result.trend[0].tolist()
        resid = result.resid[0].tolist()
        seasonal = result.seasonal[0].tolist()
        trend_list.append(trend)
        resid_list.append(resid)
        seas_list.append(seasonal)
    data_list = list({'x': px_sort[i],'y':py_sort[i],'type':'lines','name':name_sort[i]} for i in range(10))
    #data_list2 = list({'x': extra_plot_dates[0],'y':extra_plot_values[0],'type':'lines','name':"W_Filter"})
    normed_list = list({'x': px_sort[i],'y':normed[i],'type':'lines','name':name_sort[i]+"_N"} for i in range(10))
    trend_list = list({'x': px_sort[i],'y':trend_list[i],'type':'lines','name':name_sort[i]+"_T"} for i in range(10))
    return data_list + trend_list + normed_list





