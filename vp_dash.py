import sys,os,time,datetime,pickle
import dash
import dash_core_components as dcc
import dash_html_components as html
import logging
from chart_studio.plotly import plot_mpl
from statsmodels.tsa.seasonal import seasonal_decompose

logging.basicConfig()
logger = logging.getLogger('logger')
logger.warning('The system may break down')
import numpy as np
import pandas as pd

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

def dt_2_str(dt):
    return dt.strftime("%Y-%m-%d")
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
app.layout = html.Div(children=[
    html.H1(children='Vinepair Pageviews'),
    html.Div(children='Total Views'),
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
    ),
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

if __name__ == '__main__':
    app.run_server(debug=True)
