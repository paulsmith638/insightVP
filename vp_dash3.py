import sys,os,time,datetime,pickle
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import logging
from chart_studio.plotly import plot_mpl
from statsmodels.tsa.seasonal import seasonal_decompose
from datetime import datetime as dt
from vp_data import DataProc
tracked_types_csv_file="/home/paul/work/insight/vinepair/tracked_types_cats.csv"
mysql_login = "psmith"
mysql_pass = "Ins#ght2019"
mysql_host = "54.173.55.254"
mysql_port = 3306
vinepair_database = "vinepair1"

db = DataProc(mysql_login,mysql_pass,mysql_host,mysql_port,vinepair_database)
db.db_init()
db.dbsession.create_lookups()
master_ts=pickle.load(open("unique_pv_ts.pkl",'rb'))

logging.basicConfig()
logger = logging.getLogger('logger')
logger.warning('The system may break down')
import numpy as np
import pandas as pd

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

total_pagecounts = []
for k,d in master_ts.iteritems():
    for k2,v in d.iteritems():
        total = np.nansum(v.data)
        pindex = k
        total_pagecounts.append((pindex,total))

total_pagecounts.sort(key = lambda x: x[1],reverse=True)
indexed_pages = list(pc[0] for pc in total_pagecounts[0:20])
#print indexed_pages
#print indexed_data



date_dt=db.dt_list #all possible dates
n_days = len(date_dt)
date_strings = list(dt.strftime("%Y-%m-%d") for dt in date_dt)


def fill_dates(start_dt,end_dt):
    return list(start_dt + datetime.timedelta(days=i) for i in range(n_days+1))

def get_ts_dv(pindex,field):
    ddict = master_ts[pindex]
    ts = ddict[field].data
    start = ddict[field].start_dt
    end = ddict[field].end_dt
    dates = fill_dates(start,end)
    return dates,ts

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
            html.H5('Pindex'),
            dcc.Dropdown(
                id='cat-dropdown',
                options = list({"label":Pindex,"value":Pindex} for Pindex in indexed_pages),
                value=indexed_pages[0]
            ),
            html.Div(id="cat_select_output", style={'display':'none'})

        ], className="three columns",style={'width':'25%'}),
        html.Div([html.Button('Model', id='model_button')]),
    ],className="row"),
    html.Div([dcc.Graph(id='plot1')]),
    html.Div([dcc.Graph(id='plot2')]),
],
)


@app.callback(
    Output('plot1', 'figure'),[Input('cat-dropdown', 'value'),
                               Input('my-date-picker-single2', 'date'),
                               Input('my-date-picker-single1','date')])
def update_plot1(pindex,start_date,end_date):
    start_dt = dt.strptime(start_date.split(' ')[0], '%Y-%m-%d')
    end_dt = dt.strptime(end_date.split(' ')[0], '%Y-%m-%d')
    d,v = get_ts_dv(pindex,'uniquePageviews')
    plot1_xdata = d
    plot1_ydata = v
    p,t,c,s = get_page_info(pindex)
    graph_title = "[%s %s %s %s]" % (str(p),t,c,s)
    return {'data': [{'x': plot1_xdata, 'y': plot1_ydata, 'type': 'bar', 'name': 'PageViews'},],
                'layout': {'title': '%s vs. Date' % graph_title}}
@app.callback(
    Output('plot2', 'figure'),[Input('cat-dropdown', 'value'),
                               Input('my-date-picker-single2', 'date'),
                               Input('my-date-picker-single1','date'),
                               Input('model_button', 'n_clicks')])
def model_data(cat,start_date,end_date,n_clicks):
    start_dt = dt.strptime(start_date.split(' ')[0], '%Y-%m-%d')
    end_dt = dt.strptime(end_date.split(' ')[0], '%Y-%m-%d')
    ilist = get_dates_index(start_dt,end_dt)
    model_xdata = get_date_strings(ilist)
    data = list(cat_array[np.nonzero(cat_array['category'] == cat)[0]][list(str(i) for i in ilist)])
    model_ydata = indexed_data[data]
    ydat = list(model_ydata)
    
    pdf = pd.DataFrame(ydat)
    pdf.index = pd.to_datetime(model_xdata)
    result = seasonal_decompose(pdf, model='multiplicative')
    trend =  result.trend[0].tolist()
    resid = result.resid[0].tolist()
    seasonal = result.seasonal[0].tolist()
    return {'data': [{'x': model_xdata, 'y': model_ydata, 'type': 'line', 'name': 'PageViews'},
                     {'x': date_strings, 'y': trend, 'type': 'line', 'name': 'trend','color':'red'},],
            'layout': {'title': 'Trend vs. Date'}}
if __name__ == '__main__':
    app.run_server(debug=True)#, host='0.0.0.0')
