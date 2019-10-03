import sys,os,time,datetime,pickle
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import logging
from chart_studio.plotly import plot_mpl
from statsmodels.tsa.seasonal import seasonal_decompose
from datetime import datetime as dt
from vp_data import DataProc,Timeseries,AggScheme
tracked_types_csv_file="/home/paul/work/insight/vinepair/tracked_types_cats.csv"
mysql_login = "psmith"
mysql_pass = "Ins#ght2019"
mysql_host = "54.173.55.254"
mysql_port = 3306
vinepair_database = "vinepair1"

db = DataProc(mysql_login,mysql_pass,mysql_host,mysql_port,vinepair_database)
db.db_init()
db.dbsession.create_lookups()
agg1 = AggScheme()
agg1.get_agg_scheme("Default",db.dbsession.pterm_lookup,csv_in="tracked_types_cats.csv")
agg_groups = list(gdict['group_name'] for gdict in agg1.scheme['groups'])

agg_schemes = [agg1.scheme,]

logging.basicConfig()
logger = logging.getLogger('logger')
logger.warning('The system may break down')
import numpy as np
import pandas as pd

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

#total_pagecounts = []
#for k,d in master_ts.iteritems():
#    for k2,v in d.iteritems():
#        total = np.nansum(v.data)
#        pindex = k
#        total_pagecounts.append((pindex,total))

#total_pagecounts.sort(key = lambda x: x[1],reverse=True)
indexed_pages = db.dbsession.pindex_lookup.values()
#print indexed_pages
#print indexed_data

plots = []

date_dt=db.dt_list #all possible dates
n_days = len(date_dt)
date_strings = list(dt.strftime("%Y-%m-%d") for dt in date_dt)


def fill_dates(start_dt,end_dt):
    return list(start_dt + datetime.timedelta(days=i) for i in range(n_days+1))

def get_ts_dv(pindex,field):
    d,v = db.get_timeseries_pindex(pindex,'uniquePageviews')
    return d,v
    #ddict = master_ts[pindex]
    #ts = ddict[field].data
    #start = ddict[field].start_dt
    #end = ddict[field].end_dt
    #dates = fill_dates(start,end)
    #return dates,ts

def get_page_info(pindex):
    title = ""
    for k,v in db.dbsession.pindex_lookup.iteritems():
        if v == pindex:
            title=k
            break
    for term in db.dbsession.pterm_lookup[pindex]:
        tname = db.dbsession.tname_lookup.get(term,"unk-name")
        ttype = db.dbsession.ttype_lookup.get(term,"unk-type")
        if ttype == "wbs_master_taxonomy_node_cat":
            cat = tname
        else:
            cat = "UNK"
        if ttype == "wbs_master_taxonomy_node_type":
            subcat = tname
        else:
            subcat = "unk"
    return pindex,title,cat,subcat
    



#DASHBOARD
app.layout = html.Div([#L1
    html.Div([#L2
        html.Div([ #L3
            html.H5('Start Date'),
            dcc.DatePickerSingle(
                id='my-date-picker-single1',
                min_date_allowed=date_dt[0],
                max_date_allowed=date_dt[-1],
                initial_visible_month=dt(2018, 1, 1),
                date=str(dt(2018, 1, 1, 00, 00, 00))
            ),
            html.Div(id='output-container-date-picker-single1')

        ], className="three columns",style={'width':'25%'}),
        html.Div([ #L3
            html.H5('End Date'),
            dcc.DatePickerSingle(
                id='my-date-picker-single2',
                min_date_allowed=dt(2018, 1, 1),
                max_date_allowed=dt(2018, 4, 1),
                initial_visible_month=dt(2018, 4, 1),
                date=str(dt(2018, 4, 1, 00, 00, 00))
            ),
            html.Div(id='output-container-date-picker-single2')

        ], className="three columns",style={'width':'25%'}),
        html.Div([ #L3
            html.H5('Metric'),
            dcc.Dropdown(
                id='cat-dropdown1',
                options = list({"label":metric,"value":metric} for metric in db.data_keys),
                value="pageviews"
            ),
            html.Div(id="cat_select_output1", style={'display':'none'})

        ], className="three columns",style={'width':'25%'}),
        html.Div([ #L3
            html.H5('Page Group'),
            dcc.Dropdown(
                id='cat-dropdown2',
                options = list({"label":Group,"value":Group} for Group in agg_groups),
                value=indexed_pages[0]
            ),
            html.Div(id="cat_select_output2", style={'display':'none'})

        ], className="three columns",style={'width':'25%'}),
        html.Div([ #L3
            html.H5('Aggregation Scheme'),
            dcc.Dropdown(
                id='cat-dropdown3',
                options = list({"label":scheme['name'],"value":scheme['name']} for scheme in agg_schemes),
                value=agg_schemes[0]["name"]
            ),
            html.Div(id="cat_select_output3", style={'display':'none'})

        ], className="three columns",style={'width':'25%'}),
        html.Div([html.Button('Model', id='model_button')]),
    ],className="row"),
    html.Div([dcc.Graph(id='plot1',figure=plots)]),
    html.Div([dcc.Graph(id='plot2')]),
],
)

"""
@app.callback(
    Output('plot1', 'figure'),[Input('cat-dropdown1', 'value'),
                               Input('my-date-picker-single2', 'date'),
                               Input('my-date-picker-single1','date')])
def update_plot1(pindex,start_date,end_date):
    start_dt = dt.strptime(start_date.split(' ')[0], '%Y-%m-%d')
    end_dt = dt.strptime(end_date.split(' ')[0], '%Y-%m-%d')
    d,v = [],[]
    plot1_xdata = d
    plot1_ydata = v
    #p,t,c,s = get_page_info(pindex)
    #graph_title = "[%s %s %s %s]" % (str(p),t,c,s)
    graph_title = "test"
    return {'data': [{'x': plot1_xdata, 'y': plot1_ydata, 'type': 'bar', 'name': 'PageViews'},],
                'layout': {'title': '%s vs. Date' % graph_title}}

"""
@app.callback(
    Output('plot2', 'figure'),[Input('cat-dropdown1', 'value'),
                               Input('cat-dropdown2', 'value'),
                               Input('cat-dropdown3', 'value'),
                               Input('my-date-picker-single2', 'date'),
                               Input('my-date-picker-single1','date'),
                               Input('model_button', 'n_clicks')])
def plot_custom_data(metric,pgroup,scheme_name,start_date,end_date,n_clicks):
    print metric,pgroup,scheme_name,start_date,end_date,n_clicks
    start_dt = dt.strptime(start_date.split(' ')[0], '%Y-%m-%d')
    end_dt = dt.strptime(end_date.split(' ')[0], '%Y-%m-%d')
    for si,scheme in enumerate(agg_schemes):
        if scheme['name'] == scheme_name:
            agg_scheme = agg_schemes[si]
    plot_x_list = []
    plot_y_list = []
    series_names = []
    for gdict in agg_scheme['groups']:
        name = gdict['group_name']
        series_names.append(name)
        plist = gdict['group_pages']
        dates,agg_total = db.get_sumseries_plist(plist,metric)
        dates_str = list(dt.strftime("%Y-%m-%d") for dt in dates)
        plot_x_list.append(dates_str)
        plot_y_list.append(agg_total)
    #pdf = pd.DataFrame(ydat)
    #pdf.index = pd.to_datetime(model_xdata)
    #result = seasonal_decompose(pdf, model='multiplicative')
    #trend =  result.trend[0].tolist()
    #resid = result.resid[0].tolist()
    #seasonal = result.seasonal[0].tolist()
    data_list = list({'x': plot_x_list[i],'y':plot_y_list[i],'type':'line','name':series_names[i]} for i in range(len(series_names)))
    print data_list
    return {'data': data_list,
            'layout': {'title': '%s vs. Date' % metric }}

#plots = plot_custom_data("pageviews","Spirits","Default",date_dt[0],date_dt[-1],0)
if __name__ == '__main__':
    app.run_server(debug=True)#, host='0.0.0.0')
