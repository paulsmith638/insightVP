import sys,os,time,datetime,pickle
import pandas as pd
import numpy as np
from googleapiclient import discovery
from apiclient.discovery import build
from googleapiclient.http import build_http
from oauth2client.service_account import ServiceAccountCredentials
import httplib2
import sqlalchemy as sal

# Essential Variables
#location of json token for OAuth 
key_file_location = 'service.json'
#view_id for analytics property
view_id="65358754"
#path to taxonomy dictionary
tax_file = "vinepair_taxonomy.pkl"
#which fields to retreive?
ga_fields = ["sessions","pageviews","uniquePageviews","avgSessionDuration","entrances","bounceRate","exitRate"]
ga_events = ["eventCategory","eventAction","eventLabel"]
#site url for search data (can be queried also)
target_site_url="https://vinepair.com/"

#start and end dates as mm-dd-yy (does not include last day)
start_date = "01-01-19"
end_date   = "01-04-19"


#stock method for service generation
def get_service(api_name, api_version, scopes, key_file_location):
    """Get a service that communicates to a Google API.

    Args:
        api_name: The name of the api to connect to.
        api_version: The api version to connect to.
        scopes: A list auth scopes to authorize for the application.
        key_file_location: The path to a valid service account JSON key file.

    Returns:
        A service that is connected to the specified API.
    """

    credentials = ServiceAccountCredentials.from_json_keyfile_name(
            key_file_location, scopes=scopes)

    # Build the service object.
    service = build(api_name, api_version, credentials=credentials)

    return service

#converts full urls to id slugs
def link2slug(link):
    if len(link) > 0:
        ltxt = link[0]
        if len(ltxt) > 0:
            if ltxt[-1] == "/": #remove trailing slashes
                ltxt = ltxt[0:-1]
            slug = ltxt.split("/")[-1].strip()
        else:
            slug = "null"
    else:
        slug = "null"
    if len(slug) == 0:
        slug = "null"
    slug = slug.split("?")[0].strip()
    return str(slug)

#Reformat response data as Pandas dataframe
def response2df(response):
  list = []
  # get report data
  for report in response.get('reports', []):
    # set column headers
    columnHeader = report.get('columnHeader', {})
    dimensionHeaders = columnHeader.get('dimensions', [])
    metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])
    rows = report.get('data', {}).get('rows', [])
    
    for row in rows:
        # create dict for each row
        dict = {}
        dimensions = row.get('dimensions', [])
        dateRangeValues = row.get('metrics', [])

        # fill dict with dimension header (key) and dimension value (value)
        for header, dimension in zip(dimensionHeaders, dimensions):
          dict[header] = dimension

        # fill dict with metric header (key) and metric value (value)
        for i, values in enumerate(dateRangeValues):
          for metric, value in zip(metricHeaders, values.get('values')):
            #set int as int, float a float
            if ',' in value or '.' in value:
              dict[metric.get('name')] = float(value)
            else:
              dict[metric.get('name')] = int(value)

        list.append(dict)
    
    df = pd.DataFrame(list)
    return df


def get_pagedata(service,start,end):
    """
    Crafts an google analytics query that returns all sessions within the start/end date range.
    Query dimension is pagePathLevel2 (url slug)
    Metrics are taken from list at top.
    Filters restrict to USA
    """
    metrics = list(dict([("expression","ga:"+s),]) for s in ga_fields)
    results = service.reports().batchGet(
        body={
            'reportRequests': [
                {
                    'viewId': view_id,
                    'pageSize': 50000,
                    'dateRanges': [{'startDate':start, 'endDate':end}],
                    'metrics': metrics,
                    'dimensions': [{'name': 'ga:pagePathLevel2'}],
                    'dimensionFilterClauses': [{
                        'filters': [{
                            "dimensionName": "ga:country",
                            "operator": "EXACT",
                            "expressions": ["United States"]
                        }]
                    }],
                }]
        }).execute()
    df1=response2df(results)
    slugs = list(link2slug([link]) for link in df1["ga:pagePathLevel2"])
    df1['slugs'] = slugs
    df1['date'] = start
    return df1

def get_eventdata(service,start,end):
    """
    Modified from get_pagedata
    """
    dimensions = list(dict([("name","ga:"+s),]) for s in ga_events)
    results = service.reports().batchGet(
        body={
            'reportRequests': [
                {
                    'viewId': view_id,
                    'pageSize': 50000,
                    'dateRanges': [{'startDate':start, 'endDate':end}],
                    'metrics': [{'expression': 'ga:totalEvents'}],
                    'dimensions': dimensions,
                    'dimensionFilterClauses': [{
                        'filters': [{
                            "dimensionName": "ga:country",
                            "operator": "EXACT",
                            "expressions": ["United States"]
                        }]
                    }],
                }]
        }).execute()
    df1=response2df(results)
    #we're only after events labeled as "scrolled-to #2"
    valid_events = df1['ga:eventLabel'].str.match('scrolled-to #2')
    event_counts = df1['ga:totalEvents'][valid_events]
    event_labels = df1['ga:eventLabel'][valid_events]
    slugs = list(link2slug([event]) for event in event_labels)
    data = list(zip(slugs,event_counts))
    df2 = pd.DataFrame(data,columns=['slugs','scroll_events'])
    df2['date'] = start
    return df2

def get_searchdata(target_dt):
    """
    The searchconsole API uses a different search mechanism (RESTful)
    so this methods creates a new service with new scope but same credentials
    """
    date = target_dt.strftime("%Y-%m-%d")
    search_scope = 'https://www.googleapis.com/auth/webmasters.readonly'
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        key_file_location, scopes=[search_scope])
    http = httplib2.Http()
    http = credentials.authorize(http)
    service = build(
        serviceName='webmasters',
        version='v3',
        credentials=credentials,
        cache_discovery=False)
    site_list = service.sites().list().execute()
    verified_sites_urls = [s['siteUrl'] for s in site_list['siteEntry']
                           if s['permissionLevel'] != 'siteUnverifiedUser'
                           and s['siteUrl'][:4] == 'http']

    assert target_site_url in verified_sites_urls, "ERROR, access to %s not verified!" % target_site_url
    
    request = {
        'startDate': date,
        'endDate': date,
        'dimensions': ['page'],
        'rowLimit': 10000,
        'dimensionFilterGroups': [
            {"filters": [
                {
                    "dimension": "country",
                    "operator": "equals",
                    "expression": "usa"
                }
            ]
            }
        ],
    }
    response = service.searchanalytics().query(
        siteUrl=target_site_url, body=request).execute()
    if 'rows' in response:
        df1 = pd.DataFrame(response['rows'])
        slugs = list(link2slug(link) for link in df1['keys']) 
        df1['slugs'] = slugs
        df1['date'] = date

    else:
        df1= pd.DataFrame()
    request = {
        'startDate': date,
        'endDate': date,
        'dimensions': ['query'],
        'rowLimit': 10000,
        'dimensionFilterGroups': [
            {"filters": [
                {
                    "dimension": "country",
                    "operator": "equals",
                    "expression": "usa"
                }
            ]
            }
        ],
    }
    response = service.searchanalytics().query(
        siteUrl=target_site_url, body=request).execute()
    if 'rows' in response:
        df2 = pd.DataFrame(response['rows'])
        df2['date'] = date
    else:
        df2=pd.DataFrame()
    print df2
    return df1

    

def main():
    start_datetime = datetime.datetime.strptime(start_date,"%m-%d-%y")
    end_datetime = datetime.datetime.strptime(end_date,"%m-%d-%y")
    n_days = (end_datetime - start_datetime).days
    # Define the auth scopes to request.
    scope = 'https://www.googleapis.com/auth/analytics.readonly'
    # Authenticate and construct service.
    service = get_service(
        api_name='analyticsreporting',
        api_version='v4',
        scopes=[scope],
        key_file_location=key_file_location)
    #fetch analytics data by day
    pageview_df_list = []
    event_df_list = []
    search_df_list = []
    date_list = []
    for i in range(n_days):
        start = start_datetime + datetime.timedelta(days=i)
        end   = start_datetime + datetime.timedelta(days=(i+1))
        start_str = start.strftime("%Y-%m-%d")
        end_str = end.strftime("%Y-%m-%d")
        #get each batch of data
        date_list.append(start_str)
        pv_df = get_pagedata(service, start_str,end_str)
        pageview_df_list.append(pv_df)
        ev_df = get_eventdata(service, start_str,end_str)
        event_df_list.append(ev_df)
        search_df = get_searchdata(start)
        search_df_list.append(search_df)

    engine = sal.create_engine('sqlite:///analytics_data.sqlite')
    slug_list = []
    for df in pageview_df_list:
        for col in df.columns:
            df[col].to_sql(col,con=engine, if_exists='replace', index_label='id')
    sys.exit()
    for df_list in [pageview_df_list + event_df_list + search_df_list]:
        for df in df_list:
            slug_list = slug_list + list(df['slugs'])
    print "SLUGS",len(slug_list)
    unique_slugs = list(set(slug_list))
    print "UNIQUE",len(unique_slugs)
    print unique_slugs[0:100]
    sys.exit()
    merged_df = pd.concat(df_list,keys=date_list)
    clean_page_list = list(page.replace('/','').strip() for page in merged_df["ga:pagePathLevel2"])
    page_dict_list = list(master_dict.get(page,{"index":None,"tindex":[],"cat":None,
                                                "type":None}) for page in clean_page_list)
    page_index_list = list(d['index'] for d in page_dict_list)
    page_cat_list = list(d['cat'] for d in page_dict_list)
    page_type_list = list(d['type'] for d in page_dict_list)
    merged_df['page_index'] = page_index_list
    merged_df['cat'] = page_cat_list
    merged_df['type'] = page_type_list
    pickle.dump(merged_df,open("analytics_df_"+start_date+"_"+end_date+".pkl",'wb'))
    merged_df.to_csv(open("test.csv","wb"))

    #grab event data
    df_list = [] #reinitialize
    for i in range(n_days):
        start = start_datetime + datetime.timedelta(days=i)
        end   = start_datetime + datetime.timedelta(days=(i+1))
        start_str = start.strftime("%Y-%m-%d")
        end_str = end.strftime("%Y-%m-%d")
        res1 = get_eventdata(service, start_str,end_str)
        df1=response2df(res1)
        df_list.append(df1)
    merged_df = pd.concat(df_list,keys=date_list)
    test = merged_df[merged_df['ga:eventLabel'].str.match('scrolled-to #2')]['ga:totalEvents']
    print test
    #grab search data
    df_list = []
    
    
    sys.exit()

    unique_cats = tuple(set(list(v['cat'] for v in master_dict.values())))
    unique_types = tuple(set(list(v['type'] for v in master_dict.values())))
    n_cats = len(unique_cats)
    n_types = len(unique_types)
    cat_col = ["category"] + list(str(i) for i in range(n_days))
    type_col = ["type"] + list(str(i) for i in range(n_days))
    tot_fmt = ["S32"] + list("i8" for i in range(n_days))
    cat_dtype = np.dtype({'names':cat_col,"formats":tot_fmt})
    type_dtype = np.dtype({'names':type_col,"formats":tot_fmt})
    cat_array = np.zeros(n_cats,dtype=cat_dtype)
    type_array = np.zeros(n_types,dtype=type_dtype)
    cat_array['category'] = unique_cats
    type_array['type'] = unique_types
    for i in range(n_cats):
        cat_mask = np.array(list(v['cat'] == unique_cats[i] for v in master_dict.values()),dtype=np.bool_)
        for col in list(str(i) for i in range(n_days)):
            datin = pv_array[col][cat_mask]
            counts = np.nansum(datin)
            cat_array[i][col] = counts
    for i in range(n_types):
        type_mask = np.array(list(v['type'] == unique_types[i] for v in master_dict.values()),dtype=np.bool_)
        for col in list(str(i) for i in range(n_days)):
            datin = pv_array[col][type_mask]
            counts = np.nansum(datin)
            type_array[i][col] = counts
    data_out = [start_date,end_date,pv_array,cat_array,type_array]
    pickle.dump(data_out,open("analytics_by_day_"+start_date+"_"+end_date+".pkl",'wb'))
    
    print cat_array
    print type_array
    
if __name__ == '__main__':
    main()
