import sys,os,time,datetime,pickle
import pandas as pd
import numpy as np
from googleapiclient import discovery
from apiclient.discovery import build
from googleapiclient.http import build_http
from oauth2client.service_account import ServiceAccountCredentials
import httplib2

class Ingest:
    """
    A class for querying specific data from google analytics and search API's.

    Be sure "service.json" resides in the same directory as this file, is readable,
    and contains the appropriate token for authentication (JSON file)

    """
    def __init__(self):
        self.util = Utils()
        # Define the auth scopes to request.
        scope1 = 'https://www.googleapis.com/auth/analytics.readonly'
        scope2 = 'https://www.googleapis.com/auth/webmasters.readonly'
        #these are specific to vinepair.com
        #####################################
        self.view_id="65358754"
        self.target_url="https://vinepair.com/"
        key_file_location = 'service.json'
        #####################################
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
                key_file_location, scopes=[scope1,scope2])
        self.ga_service = self.get_ga_service(credentials)
        self.sc_service = self.get_sc_service(credentials)
 

    def get_ga_service(self,credentials):
        """
        Get a service that communicates to the Google Analytics API
        """
        api_name='analyticsreporting'
        api_version='v4'
        service = build(api_name, api_version, credentials=credentials)

        return service


    def get_sc_service(self,credentials):
        """
        Get a service that communicates to the Google Search Console API (RESTful)
        """
        http = httplib2.Http()
        http = credentials.authorize(http)
        service = build(serviceName='webmasters',version='v3',
                        credentials=credentials, cache_discovery=False)
        return service


    def get_pageviews(self,metrics,start,end,filter=True):
        """
        Crafts an google analytics query that returns all sessions within the start/end date range.
        Query dimension is pagePathLevel2 (url slug)
        Filters restrict to USA
        """
        if filter:
            filters = [{"dimensionName": "ga:country",
                        "operator": "EXACT",
                        "expressions": ["United States"]}]
        else:
            filters = []
        results = self.ga_service.reports().batchGet(
            body={
                'reportRequests': [
                    {
                        'viewId': self.view_id,
                        'pageSize': 50000,
                        'dateRanges': [{'startDate':start, 'endDate':end}],
                        'metrics': metrics,
                        'dimensions': [{'name': 'ga:pagePathLevel2'}],
                        'dimensionFilterClauses': [{'filters': filters}],
                    }]
            }).execute()
        df1=self.util.response2df(results)
        slugs = list(self.util.link2slug([link]) for link in df1["ga:pagePathLevel2"])
        df1['slug'] = slugs
        df1['date'] = start
        return df1

    def get_events(self,start,end,filter=True):
        """
        Crafts an google analytics query that returns all event counts matching "scrolled-to #2"
        for each page/url/slug
        Filters restrict to USA
        """
        ga_events = ["eventCategory","eventAction","eventLabel"]
        dimensions = list(dict([("name","ga:"+s),]) for s in ga_events)
        if filter:
            filters = [{"dimensionName": "ga:country",
                        "operator": "EXACT",
                        "expressions": ["United States"]}]
        else:
            filters = []
        results = self.ga_service.reports().batchGet(
            body={
                'reportRequests': [
                    {
                        'viewId': self.view_id,
                        'pageSize': 50000,
                        'dateRanges': [{'startDate':start, 'endDate':end}],
                        'metrics': [{'expression': 'ga:totalEvents'}],
                        'dimensions': dimensions,
                        'dimensionFilterClauses': [{'filters': filters}],
                    }]
            }).execute()
        df1=self.util.response2df(results)
        #we're only after events labeled as "scrolled-to #2"
        valid_events = df1['ga:eventLabel'].str.match('scrolled-to #2')
        event_counts = df1['ga:totalEvents'][valid_events]
        event_labels = df1['ga:eventLabel'][valid_events]
        slugs = list(self.util.link2slug([event]) for event in event_labels)
        data = list(zip(slugs,event_counts))
        df2 = pd.DataFrame(data,columns=['slug','scroll_events'])
        df2['date'] = start
        return df2


    def get_searchdata(self,dimension,date,filter=True):
        """
        The search console API uses a different search mechanism (RESTful)
        requires a different service, 
        date is YYYY-mm-dd string
        target_url is the TLD for the website (e.g. https://vinepair.com/)
        dimension is "page" or "query"
        """

        #double check that we're authenticated to the domain
        site_list = self.sc_service.sites().list().execute()
        verified_sites_urls = [s['siteUrl'] for s in site_list['siteEntry']
                               if s['permissionLevel'] != 'siteUnverifiedUser'
                               and s['siteUrl'][:4] == 'http']

        assert self.target_url in verified_sites_urls, "ERROR, access to %s not verified!" % target_site_url
        if filter:
            filters = [{"dimension": "country",
                        "operator": "equals",
                        "expression": "usa"}]
        else:
            filters = []
        request = {
            'startDate': date,
            'endDate': date,
            'dimensions': [dimension],
            'rowLimit': 20000,
            'dimensionFilterGroups': [
                {"filters": filters
                }
            ],
        }
        response = self.sc_service.searchanalytics().query(
            siteUrl=self.target_url, body=request).execute()
        if 'rows' in response:
            df1 = pd.DataFrame(response['rows'])
            slugs = list(self.util.link2slug(link) for link in df1['keys']) 
            df1['slug'] = slugs
            df1['date'] = date

        else:
            #search data goes back only ~16mo, if not there, return empty dataframe
            df1= pd.DataFrame()
        return df1


    
    def get_google_data(self,db_session,dt_list,pindex_lookup,targets=["pv","ev","sp","st"]):
        """
        Ingests data by calling API query functions above. 
        db_session = mysql connection
        dt_list = list of datetimes over which to iterate
        pindex_lookup = dict for converting slugs to page index
        targets = list of which data to get, pv=pageviews, ev=events, sp=search pages, st=search terms
        """
        ga_fields = ["sessions","pageviews","uniquePageviews","avgSessionDuration","entrances","bounceRate","exitRate"]
        search_fields = ["clicks","ctr","impressions","position"]
        ga_metrics = list({"expression":"ga:"+field} for field in ga_fields)
        for dt in dt_list:
            date_str = dt.strftime("%Y-%m-%d")
            sql_timestr = dt.strftime("%Y-%m-%d %H-%M-%S")
            # PAGEVIEWS
            if "pv" in targets:
                print "Fetching Pageview data for:",date_str
                try:
                    pv_df = self.get_pageviews(ga_metrics,date_str,date_str)
                    time.sleep(1) # slow down API queries
                except: #try again for failed connection
                    time.sleep(60)
                    pv_df = self.get_pageviews(ga_metrics,date_str,date_str)
                    time.sleep(1) # slow down API queries
                for field in ga_fields:
                    db_session.store_pv_df(pv_df,field,sql_timestr,"pageviews")

            # EVENTS
            if "ev" in targets:
                print "Fetching Event Data",date_str
                field="scroll_events"
                try:
                    ev_df = self.get_events(date_str,date_str)
                    db_session.store_pv_df(ev_df,field,sql_timestr,"events")
                except:
                    time.sleep(60) # slow down API queries
                    ev_df = self.get_events(date_str,date_str)
                    db_session.store_pv_df(ev_df,field,sql_timestr,"events")
            #SEARCH PAGES
            if "sp" in targets:
                print "Fetching Search Data by Page",date_str
                try:
                    sc_df = self.get_searchdata("page",date_str)
                except:
                    time.sleep(60)
                    sc_df = self.get_searchdata("page",date_str)
                for field in search_fields:
                    print "Storing data for",date_str,field
                    db_session.store_pv_df(sc_df,field,sql_timestr,"search")

            if "st" in targets:
                print "Fetching Search Data by Query",date_str
                try:
                    scp_df = self.get_searchdata("query",date_str)
                except:
                    time.sleep(60)
                    scp_df = self.get_searchdata("query",date_str)
                #if search data is null or missing
                if scp_df.shape[0] == 0:
                    continue
                #these dataframes contain alot of data with few clicks and/or impressions
                #filter out both
                low_click = scp_df['clicks'] < 1
                low_impress = scp_df['impressions'] < 50
                mask = np.logical_or(low_click,low_impress)
                scp_df = scp_df.loc[np.invert(mask),:]
                for field in search_fields:
                    print "Storing Term data for",date_str,field
                    db_session.store_pv_df(scp_df,field,sql_timestr,"searchterm")

    

class Utils:
    """
    A class of utility functions for consistently handling data
    """
    def __init__(self):
        pass

    def link2slug(self,link):
        """
        Takes a url like "vinepair.com/articles/best_wine?urlterms" and returns
        the "slug" that represents the unique content title "best_wine"
        """
        if type(link) == type([]): #some text comes in a list
            link = link[0].strip()
        if len(link) > 0: #if nonzero string
            if link[-1] == "/": #remove trailing slashes
                link = link[0:-1]
            slug = link.split("/")[-1].strip()
        else:
            slug = "null"
        if len(slug) == 0:
            slug = "null"
        slug = slug.split("?")[0].strip()
        if len(slug) == 0:
            slug = "null"
        return slug.encode('utf-8','ignore')


    #Reformat response data as Pandas dataframe
    def response2df(self,response):
        """
        Converts Query to pandas DataFrame
        dictionary keys are column names, as is
        """
        dlist = []
        # get report data
        for report in response.get('reports', []):
            # set column headers
            columnHeader = report.get('columnHeader', {})
            dimensionHeaders = columnHeader.get('dimensions', [])
            metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])
            rows = report.get('data', {}).get('rows', [])
    
            for row in rows:
                # create dict for each row
                ddict = {}
                dimensions = row.get('dimensions', [])
                dateRangeValues = row.get('metrics', [])

                # fill dict with dimension header (key) and dimension value (value)
                for header, dimension in zip(dimensionHeaders, dimensions):
                    ddict[header] = dimension

                # fill dict with metric header (key) and metric value (value)
                for i, values in enumerate(dateRangeValues):
                    for metric, value in zip(metricHeaders, values.get('values')):
                        #set int as int, float a float
                        if ',' in value or '.' in value:
                            ddict[metric.get('name')] = float(value)
                        else:
                            ddict[metric.get('name')] = int(value)

                dlist.append(ddict)
    
        df = pd.DataFrame(dlist)
        return df

