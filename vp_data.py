import sys,os,time,datetime,pickle
from statsmodels.tsa.seasonal import seasonal_decompose
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import Column, Integer, Float,String, DateTime,MetaData,Table
from sqlalchemy.ext.declarative import declarative_base
from pv_ingest import Utils

tracked_types_csv_file="/home/paul/work/insight/vinepair/tracked_types_cats.csv"


mysql_login = "psmith"
mysql_pass = "Ins#ght2019"
mysql_port = 3306

vinepair_database = "vinepair1"
sql_connect  =  'mysql://%s:%s@localhost:%s/%s' % (mysql_login,mysql_pass,mysql_port,vinepair_database)
engine = create_engine(sql_connect) 
Base = declarative_base()
session = Session(engine)

#reconstruct dictionaries from SQL
pindex_query = session.execute("SELECT slug,pindex FROM pindex")
pindex_lookup = {}
for result in pindex_query:
    if result[0] != "null":
        pindex_lookup[result[0]] = result[1]

pterm_query = session.execute("SELECT pindex,termstr FROM pterms")
pterm_lookup = {}
for result in pterm_query:
    termstr = result[1]
    tlist = termstr.split(',')
    tlist = list(int(term.strip()) for term in tlist)
    pterm_lookup[result[0]] = tlist

tname_lookup = {}
tname_query = session.execute("SELECT tindex,term FROM tindex")
for result in tname_query:
    tname_lookup[result[0]] = result[1]


ttype_lookup = {}
ttype_query = session.execute("SELECT tindex,ttype FROM ttype")
for result in ttype_query:
    ttype_lookup[result[0]] = result[1]

"""
for slug,pindex in pindex_lookup.iteritems():
    tlist = pterm_lookup.get(pindex,[])
    tnames = list(tname_lookup.get(term,"null1") for term in tlist)
    ttypes = list(ttype_lookup.get(term,"null2") for term in tlist)
    types = zip(tnames,ttypes)
    for tdesc in types:
        print 'PAGE "%s" associated with TERM: "%s" which is a "%s" tag' % (slug,tdesc[0],tdesc[1])
"""

track_lookup={}
track_f = open(tracked_types_csv_file,'r')
for ln,line in enumerate(track_f):
    if ln > 0:
        fields = line.split(',')
        if len(fields) > 1:
            cat = fields[0]
            ids = fields[1::]
            for tindex in ids:
                tdig=""
                for letter in tindex:
                    if letter.isdigit():
                        tdig=tdig+letter
                if len(tdig)>0:
                    track_lookup[int(tdig)] = cat.replace('"','').strip()
track_f.close()

sys.exit()
for ddict in (pindex_lookup,pterm_lookup,tname_lookup,ttype_lookup):
    for k,v in ddict.iteritems():
        print k,v
    
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
