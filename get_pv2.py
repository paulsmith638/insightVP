import sys,os,time,datetime,pickle
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import Column, Integer, Float,String, DateTime,MetaData,Table
from sqlalchemy.ext.declarative import declarative_base
from pv_ingest import Ingest
from pv_ingest import Utils



#instantialize classes
ingest = Ingest()
util = Utils()

mysql_login = "psmith"
mysql_pass = "Ins#ght2019"
mysql_port = 3306

vinepair_database = "vinepair1"
sql_connect  =  'mysql://%s:%s@localhost:%s/%s' % (mysql_login,mysql_pass,mysql_port,vinepair_database)
engine = create_engine(sql_connect) 
Base_write = declarative_base()
write_session = Session(engine)
if not engine.dialect.has_table(engine, "pagedata"): 
    metadata = MetaData(engine)
    Table("pagedata", metadata,
          Column('id', Integer, primary_key=True,nullable=False),
          Column('pindex',Integer),
          Column('date', DateTime), 
          Column('key', String(length=256)),
          Column('count',Float))
    # Implement the creation
    metadata.create_all()

# Essential Variables

#location of json token for OAuth 
key_file_location = 'service.json'

#view_id for analytics property
view_id="65358754"

#which fields to retreive (metrics)
ga_fields = ["sessions","pageviews","uniquePageviews","avgSessionDuration","entrances","bounceRate","exitRate"]
search_fields = ["clicks","ctr","impressions","position"]
#site url for search data (can be queried also)
target_site_url="https://vinepair.com/"

#start and end dates as mm-dd-yy (does not include last day)
start_date = "11-09-18"
end_date   = "09-25-19"

#instantiate classes
ingest = Ingest()
util = Utils()

class Pagedata(Base_write):
    __tablename__ = "pagedata"
    id = Column(Integer, primary_key=True)
    pindex = Column(Integer)
    key = Column(String(length=256))
    date = Column(DateTime)
    count = Column(Float)
    

def main():
    start_datetime = datetime.datetime.strptime(start_date,"%m-%d-%y")
    end_datetime = datetime.datetime.strptime(end_date,"%m-%d-%y")
    n_days = (end_datetime - start_datetime).days
    # Define the auth scopes to request.
    scope1 = 'https://www.googleapis.com/auth/analytics.readonly'
    scope2 = 'https://www.googleapis.com/auth/webmasters.readonly'
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
            key_file_location, scopes=[scope1,scope2])
    ga_service = ingest.get_ga_service(credentials)
    sc_service = ingest.get_sc_service(credentials)
    pindex_query = write_session.execute("SELECT slug,pindex FROM pindex")
    pindex_lookup = {}
    for result in pindex_query:
        if result[0] != "null":
            pindex_lookup[result[0]] = result[1]
    for i in range(n_days):
        start = start_datetime + datetime.timedelta(days=i)
        end   = start_datetime + datetime.timedelta(days=(i+1))
        start_str = start.strftime("%Y-%m-%d")
        end_str = end.strftime("%Y-%m-%d")
        sql_timestr = start.strftime("%Y-%m-%d %H-%M-%S")
        #get each batch of data
        for field in ga_fields:
            print "Fetching data for:",field,start_str
            pv_df = ingest.get_pageviews(ga_service,view_id,field,start_str,end_str)
            for idx,row in pv_df.iterrows():
                slug = row["slug"]
                if slug=="null":
                    continue

                pindex = pindex_lookup.get(slug,-1)
                count = float(row["ga:"+field])
                if pindex > 0:
                    record = Pagedata(pindex=pindex,key=field,count=count,date=sql_timestr)
                    write_session.add(record)
            write_session.commit()
            time.sleep(1) # slow down API queries
        time.sleep(10) # slow down API queries
        print "Fetching Event Data",start_str
        ev_df = ingest.get_events(ga_service,view_id,start_str,end_str)
        for idx,row in ev_df.iterrows():
            slug = row["slug"]
            if slug=="null":
                continue

            pindex = pindex_lookup.get(slug,-1)
            count = float(row["scroll_events"])
            if pindex > 0:
                record = Pagedata(pindex=pindex,key="scroll_events",count=count,date=sql_timestr)
                write_session.add(record)
        write_session.commit()
        time.sleep(61) # slow down API queries
        sc_df = ingest.get_searchdata(sc_service,target_site_url,"page",start_str)
        for field in search_fields:
            print "Fetching data for:",field,start_str
            for idx,row in sc_df.iterrows():
                slug = row["slug"]
                if slug=="null":
                    continue
                pindex = pindex_lookup.get(slug,-1)
                count = float(row[field])
                if pindex > 0:
                    record = Pagedata(pindex=pindex,key=field,count=count,date=sql_timestr)
                    write_session.add(record)
            write_session.commit()
            time.sleep(1) # slow down API queries

if __name__ == '__main__':
    main()
