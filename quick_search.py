import sys,os,time,datetime,pickle
import pandas as pd
import numpy as np
from googleapiclient import discovery
from apiclient.discovery import build
from googleapiclient.http import build_http
from oauth2client.service_account import ServiceAccountCredentials
import httplib2

#Essential Variables
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
    return str(slug)

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
        siteUrl=verified_sites_urls[0], body=request).execute()
    if 'rows' in response:
        df1 = pd.DataFrame(response['rows'])
        slugs = list(link2slug(link) for link in df1['keys']) 
        df1['keys'] = slugs
        df1['date'] = date
        print df1
        return df1
    else:
        return pd.DataFrame()

def main():
    #read in taxonomy
    master_dict = pickle.load(open(tax_file,"rb"))
    start_datetime = datetime.datetime.strptime(start_date,"%m-%d-%y")
    end_datetime = datetime.datetime.strptime(end_date,"%m-%d-%y")
    n_days = (end_datetime - start_datetime).days
    n_pages = len(master_dict)
    
    start = start_datetime
    search_df = get_searchdata(start)
    
if __name__ == '__main__':
    main()
