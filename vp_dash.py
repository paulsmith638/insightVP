import sys,os,time,datetime,pickle
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import logging
from chart_studio.plotly import plot_mpl
from statsmodels.tsa.seasonal import seasonal_decompose
from datetime import datetime as dt
from vp_data import DBsession
tracked_types_csv_file="/home/paul/work/insight/vinepair/tracked_types_cats.csv"
mysql_login = "psmith"
mysql_pass = "Ins#ght2019"
mysql_port = 3306
vinepair_database = "vinepair1"

db = DBsession(mysql_login,mysql_pass,mysql_port,vinepair_database)


logging.basicConfig()
logger = logging.getLogger('logger')
logger.warning('The system may break down')
import numpy as np
import pandas as pd

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

def dt_2_str(dt):
    return dt.strftime("%Y-%m-%d")
plist = db.tracked_tindex_pages[1584]
db.timeseries_as_numpy()
sys.exit()
start_date,end_date,pv_array,cat_array,type_array=pickle.load(open(sys.argv[1],"rb"))
start_datetime = datetime.datetime.strptime(start_date,"%m-%d-%y")
end_datetime = datetime.datetime.strptime(end_date,"%m-%d-%y")
n_days = (end_datetime - start_datetime).days
date_strings = list(dt_2_str(start_datetime + datetime.timedelta(days=i)) for i in range(n_days))
date_dt = list(start_datetime + datetime.timedelta(days=i) for i in range(n_days))


total_visits = list(np.nansum(pv_array[str(i)]) for i in range(n_days))
cat_names = list(cat_array['category'])
type_names = list(type_array['type'])
cat_data = list(list(row[str(i)] for i in range(n_days)) for row in cat_array)
type_data = list(list(row[str(i)] for i in range(n_days)) for row in type_array)
type_counts = list(np.nansum(tcounts) for tcounts in type_data)
to_sort = np.argsort(type_counts)[::-1]
type_data_sorted = list(type_data[i] for i in to_sort)
type_names_sorted = list(type_names[i] for i in to_sort)



pv_raw = np.zeros((pv_array.shape[0],n_days),dtype=np.int64)
for index,column in enumerate(list(str(i) for i in range(n_days))):
    pv_raw[:,index] = pv_array[column]
pv_totals = np.nansum(pv_raw.astype(np.int64),axis=1)
pv_sorted = None
whiskey = list(type_array[np.nonzero(type_array['type'] == 'whiskey')[0][0]][list(str(i) for i in range(n_days))])
pdf = pd.DataFrame(whiskey)
pdf.index = pd.to_datetime(date_strings)
result = seasonal_decompose(pdf, model='multiplicative')
trend =  result.trend[0].tolist()
resid = result.resid[0].tolist()
seasonal = result.seasonal[0].tolist()

def get_dates_index(start_dt,end_dt):
    ilist = []
    for index,dt1 in enumerate(date_dt):
        if dt1 <= start_dt and dt1 >= end_dt:
            ilist.append(index)
    return ilist
def get_date_strings(ilist):
    dlist = list(date_str for i,date_str in enumerate(date_strings) if i in ilist)
    return dlist

def get_y_data(data,ilist):
    data = list(data)
    if len(data) == 1:
        data = data[0]
    dlist = list(datum for i,datum in enumerate(data) if i in ilist)
    return dlist



start_dt = start_datetime
end_dt = end_datetime
plot1_xdata = date_strings
plot1_ydata = total_visits


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
            html.H5('Category'),
            dcc.Dropdown(
                id='cat-dropdown',
                options = list({"label":cat,"value":cat} for cat in cat_names),
                value=cat_names[3]
            ),
            html.Div(id="cat_select_output", style={'display':'none'})

        ], className="three columns",style={'width':'25%'}),
        html.Div([html.Button('Model', id='model_button')]),
    ],className="row"),
    html.Div([dcc.Graph(id='plot1')]),
    html.Div([dcc.Graph(id='plot2')]),
],
)


"""
app.layout = html.Div(children=[
    html.H1(children='Vinepair Pageviews'),
    html.Div([
#        children='Total Views',
        dcc.Graph(
            id='pageviews',
            figure={
                'data': [
                    {'x': date_strings, 'y': total_visits, 'type': 'bar', 'name': 'Max Views'},
                    {'x': date_strings, 'y': total_visits, 'type': 'line', 'name': 'Max Views'},
                ],
                'layout': {
                    'title': 'Total Pageviews vs. Date'
                }
            }
        )
    ],
    html.Div(children='Categories'),
    dcc.Graph(
        id='Categories',
        figure={
            'data': [
                {'x': date_strings, 'y': cat_data[0], 'type': 'line', 'name': cat_names[0]},
                {'x': date_strings, 'y': cat_data[1], 'type': 'line', 'name': cat_names[1]},
                {'x': date_strings, 'y': cat_data[2], 'type': 'line', 'name': cat_names[2]},
                {'x': date_strings, 'y': cat_data[3], 'type': 'line', 'name': cat_names[3]},
                {'x': date_strings, 'y': cat_data[4], 'type': 'line', 'name': cat_names[4]},
                {'x': date_strings, 'y': cat_data[5], 'type': 'line', 'name': cat_names[5]},
                {'x': date_strings, 'y': cat_data[6], 'type': 'line', 'name': cat_names[6]},
                
            ],
            'layout': {
                'title': 'Pageviews by Category'
            }
        }
    ),
    html.Div(children='Types'),
    dcc.Graph(
        id='Types',
        figure={
            'data': list({'x': date_strings, 'y': type_data_sorted[i], 'type': 'line', 'name': type_names_sorted[i]} for i in range(10) if type_names_sorted[i] != "unk"),
            'layout': {
                'title': 'Pageviews by Beverage Type'
            }
        }
    ),
    html.Div(children='Whiskey'),
    dcc.Graph(
        id='Whiskey',
        figure={
            'data': [
                {'x': date_strings, 'y': whiskey, 'type': 'line', 'name': "Whiskey"},
            ],
            'layout': {
                'title': 'Whiskey Data'
            }
        }
    ),
    html.Div(children='Whiskey_Trends'),
    dcc.Graph(
        id='Whiskey_Trend',
        figure={
            'data': [
                {'x': date_strings, 'y': trend, 'type': 'line', 'name': "Trend"}
            ],
            'layout': {
                'title': 'Whiskey Trend'
            }
        }
    ),
    html.Div(children='Whiskey_Resid'),
    dcc.Graph(
        id='Whiskey_Resid',
        figure={
            'data': [
                {'x': date_strings, 'y': resid, 'type': 'line', 'name': "Resid"},
            ],
            'layout': {
                'title': 'Whiskey Residuals'
            }
        }
    ),
    html.Div(children='Whiskey_Seasonal'),
    dcc.Graph(
        id='Whiskey_Seasonal',
        figure={
            'data': [
                {'x': date_strings, 'y': seasonal, 'type': 'line', 'name': "Seasonal"},
            ],
            'layout': {
                'title': 'Whiskey Seasonal'
            }
        }
    )
    
])

@app.callback(
    Output('output-container-date-picker-single1', 'children'),[Input('my-date-picker-single1', 'date')])
def update_start_date(date):
    if date is not None:
        date = dt.strptime(date.split(' ')[0], '%Y-%m-%d')
        update_plot1(plot1_ydata,date,end_dt)
        date_string = date.strftime('%B %d, %Y')
        return date_string
@app.callback(
    Output('output-container-date-picker-single2', 'children'),[Input('my-date-picker-single2', 'date')])
def update_end_date(date):
    if date is not None:
        date = dt.strptime(date.split(' ')[0], '%Y-%m-%d')
        date_string = date.strftime('%B %d, %Y')
        return date_string
"""
@app.callback(
    Output('plot1', 'figure'),[Input('cat-dropdown', 'value'),
                               Input('my-date-picker-single2', 'date'),
                               Input('my-date-picker-single1','date')])
def update_plot1(cat,start_date,end_date):
    start_dt = dt.strptime(start_date.split(' ')[0], '%Y-%m-%d')
    end_dt = dt.strptime(end_date.split(' ')[0], '%Y-%m-%d')
    ilist = get_dates_index(start_dt,end_dt)
    plot1_xdata = get_date_strings(ilist)
    data = list(cat_array[np.nonzero(cat_array['category'] == cat)[0]][list(str(i) for i in ilist)])
    plot1_ydata = get_y_data(data,ilist)
    return {'data': [{'x': plot1_xdata, 'y': plot1_ydata, 'type': 'bar', 'name': 'PageViews'},],
                'layout': {'title': '%s vs. Date' % cat.upper()}}
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
    model_ydata = get_y_data(data,ilist)
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
    app.run_server(debug=False, host='0.0.0.0')
