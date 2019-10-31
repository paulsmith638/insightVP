import sys,os,time
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import Column, Integer, Float,String,DateTime,MetaData,Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy_utils import database_exists,create_database
from pv_ingest import Ingest
from pv_ingest import Utils
from vp_prop import vinepair_creds

#instantialize classes
ingest = Ingest()
util = Utils()
creds = vinepair_creds()

Base_write = declarative_base()

# class definitions for sqlalchemy
class Slug(Base_write):
    __tablename__ = 'pindex'
    id = Column(Integer, primary_key=True)
    slug = Column(String(length=256),index=True)
    pindex = Column(Integer)
    def __repr__(self):
        return self.slug

class Term(Base_write):
    __tablename__ = 'tindex'
    id = Column(Integer, primary_key=True)
    term = Column(String(length=256))
    tindex = Column(Integer,index=True)
    def __repr__(self):
        return self.term
    
class Type(Base_write):
    __tablename__ = 'ttype'
    id = Column(Integer, primary_key=True)
    ttype = Column(String(length=256))
    tindex = Column(Integer,index=True)
    def __repr__(self):
        return self.ttype

class PageTerms(Base_write):
    __tablename__ = 'pterms'
    id = Column(Integer, primary_key=True)
    termstr = Column(String(length=4096),default="")
    pindex = Column(Integer,index=True)
    def __repr__(self):
        return self.termstr
    
class Pagedata(Base_write):
    __tablename__ = "pagedata"
    id = Column(Integer, primary_key=True)
    pindex = Column(Integer)
    key = Column(String(length=256))
    date = Column(DateTime)
    count = Column(Float)

class Stermdata(Base_write):
    __tablename__ = "searchdata"
    id = Column(Integer, primary_key=True)
    key = Column(String(length=256))
    sterm = Column(String(length=256))
    date = Column(DateTime)
    count = Column(Integer)

class DBsession:
    def __init__(self,mysql_login,mysql_pass,mysql_host,mysql_port,database):
        self.login = mysql_login
        self.password = mysql_pass
        self.port = mysql_port
        sql_connect  =  'mysql://%s:%s@%s:%s/%s' % (mysql_login,mysql_pass,mysql_host,mysql_port,database)
        self.engine = create_engine(sql_connect) 
        self.Base = declarative_base()
        self.Base.metadata.create_all(self.engine)
        self.session = Session(self.engine)
        print "Created Database Session:",database
        self.pindex_lookup = None
        self.pterm_lookup = None
        self.tname_lookup = None
        self.ttype_lookup = None


    def create_lookups(self):
        """
        reconstruct dictionaries from SQL
        pindex_lookup: slug (str) --> pindex (page identifier int)
        pterm_lookup: pindex (int) --> pterms (terms associated with a page, serialized list)
        tname_lookup: tindex (int) --> term (name of term str)
        ttype_lookup: tindex (int) --> ttype (taxonomy tag for term str)
        """
        pindex_query = self.session.execute("SELECT slug,pindex FROM pindex")
        self.pindex_lookup = {}
        for result in pindex_query:
            if result[0] != "null":
                self.pindex_lookup[result[0]] = result[1]

        pterm_query = self.session.execute("SELECT pindex,termstr FROM pterms")
        self.pterm_lookup = {}
        for result in pterm_query:
            termstr = result[1]
            tlist = termstr.split(',')
            tlist = list(int(term.strip()) for term in tlist)
            self.pterm_lookup[result[0]] = tlist

        self.tname_lookup = {}
        tname_query = self.session.execute("SELECT tindex,term FROM tindex")
        for result in tname_query:
            self.tname_lookup[result[0]] = result[1]


        self.ttype_lookup = {}
        ttype_query = self.session.execute("SELECT tindex,ttype FROM ttype")
        for result in ttype_query:
            self.ttype_lookup[result[0]] = result[1]


    def ingest_wordpress_tax(self,wordpress_db):
        sql_connect  =  'mysql://%s:%s@localhost:%s/%s' % (self.login,self.password,self.port,wordpress_db)
        read_engine = create_engine(sql_connect)
        print "Connected to WP database:",wordpress_db
        if not database_exists(self.engine.url):
            create_database(self.engine.url)    
        for table in ('pindex','tindex','ttype','pterms'):
            self.session.execute("DROP TABLE IF EXISTS %s" % table)
        meta = MetaData(self.engine)
        Table('pindex', meta ,Column('id', Integer, primary_key = True), 
              Column('slug', String(length=256)), Column('pindex', Integer))
        Table('tindex', meta ,Column('id', Integer, primary_key = True), 
              Column('term', String(length=256)), Column('tindex', Integer))
        Table('ttype', meta ,Column('id', Integer, primary_key = True), 
              Column('ttype', String(length=256)), Column('tindex', Integer))
        Table('pterms', meta ,Column('id', Integer, primary_key = True), 
              Column('termstr', String(length=4096)), Column('pindex', Integer))
        meta.create_all(self.engine)
        Base_read = automap_base()
        Base_read.prepare(read_engine, reflect=True)
        read_session = Session(read_engine)
        Base_read.metadata.create_all(read_engine)

        #pindex
        Pages= Base_read.classes.wp_po_plugins
        pages = read_session.query(Pages)
        for page in pages:
            slug = util.link2slug([page.permalink])
            index = page.post_id
            record = Slug(slug=slug,pindex=index)
            self.session.add(record)
        self.session.commit()

        #pterms
        Pages = Base_read.classes.wp_term_relationships
        pages = read_session.query(Pages)
        #first store as dictionary of lists
        pterm_lookup = {}
        for page in pages:
            pindex=page.object_id
            tindex=page.term_taxonomy_id
            pterm_list = pterm_lookup.get(pindex,[])
            pterm_list.append(tindex)
            pterm_lookup[pindex] = pterm_list
        #next, convert lists to strings and store
        for pindex,tlist in pterm_lookup.iteritems():
            termstr = ",".join(str(term) for term in tlist)
            record=PageTerms(pindex=pindex,termstr=termstr)
            self.session.add(record)
        self.session.commit()

        #tindex
        Pages = Base_read.classes.wp_terms
        pages = read_session.query(Pages)
        for page in pages:
            tindex = page.term_id
            tname = page.name
            record = Term(tindex=tindex,term=tname)
            self.session.add(record) 
        self.session.commit()

        #ttypes
        Pages = Base_read.classes.wp_term_taxonomy
        pages = read_session.query(Pages)
        for page in pages:
            tindex = page.term_id
            ttype= page.taxonomy
            record = Type(tindex=tindex,ttype=ttype)
            self.session.add(record)
        self.session.commit()

        print "Created pindex,pterm,tindex,ttypes"

    
    def index_columns(self):
        check_tables = list(tup[0] for tup in self.session.execute("SHOW TABLES").fetchall())
        for table in ("pagedata","searchdata"):
            if table not in check_tables:
                print "Error: %s table does not exist!" % table
            else:
                check_idx = self.session.execute("SHOW INDEX FROM %s" % table).fetchall()
                check_keys = set(list(tup[2][4::] for tup in check_idx))
                if table == "pagedata":
                    mterm = "pindex"
                else:
                    mterm = "sterm"
                col_list = (mterm,'date','key')
                #check/create single key columns
                for col in col_list:
                    if col not in check_keys:
                        if col == "key":
                            col = "`key`"
                        cstr = col.replace('`',"")
                        print "Creating index on:",cstr
                        sql = "CREATE INDEX idx_%s ON %s(%s)" % (cstr,table,col)
                        self.session.execute(sql)
                #add composite column keys
                for i in range(len(col_list)):
                    for j in range(i+1,len(col_list)):
                        col1 = col_list[i]
                        col2 = col_list[j]
                        if col1 == "key":
                            col1 = "`key`"
                        if col2 == "key":
                            col2 = "`key`"
                        comp = col1+","+col2
                        cstr = col_list[i]+"_"+col_list[j]
                        cstr = cstr.replace('`',"")
                        if cstr not in check_keys:
                            print "Creating index on:",cstr
                            sql = "CREATE INDEX idx_%s ON %s(%s)" % (cstr,table,comp)
                            self.session.execute(sql)
                all_col = mterm+",date,`key`"
                all_str = "_".join(col_list)
                if all_str not in check_keys:
                    print "Creating index on:",all_str
                    sql = "CREATE INDEX idx_%s ON %s(%s)" % (all_str,table,all_col)
                    self.session.execute(sql)
                    
    def deduplicate_sql(self):
        """
        ingest may duplicate values, keep values with max id (latest added)
        """
        for table in ("pagedata","searchdata"):
            if table == "pagedata":
                mterm = "pindex"
                print "Removing Duplicates from pagedata"
            else:
                mterm = "sterm"
                print "Removing Duplicates from searchdata"
            
            sql = "DELETE FROM %s WHERE id IN " % table + \
                  "(WITH duplicates AS (SELECT MAX(id) AS " + \
                  "lastId,%s,date,`key` FROM %s GROUP BY %s,date,`key` HAVING count(*) > 1)," % (mterm,table,mterm) + \
                  "dupId AS (SELECT id FROM %s INNER JOIN duplicates on %s.%s = duplicates.%s " % (table,table,mterm,mterm) + \
                  "AND %s.date = duplicates.date AND %s.`key` = duplicates.`key` " % (table,table) + \
                  "WHERE %s.id < duplicates.lastId) SELECT * FROM dupId)" % table
            print sql
            #self.session.execute(sql)

            
    def store_pv_df(self,pv_df,field,sql_timestr,frame_type):
        if pv_df.shape[0] == 0:
            return
        if frame_type == "pageviews":
            for idx,row in pv_df.iterrows():
                slug = row["slug"]
                if slug=="null":
                    continue
                pindex = self.pindex_lookup.get(slug,-1)
                count = float(row["ga:"+field])
                if pindex > 0:
                    record = Pagedata(pindex=pindex,key=field,count=count,date=sql_timestr)
                    self.session.add(record)
            self.session.commit()
        if frame_type == "events":
            for idx,row in pv_df.iterrows():
                slug = row["slug"]
                if slug=="null":
                    continue
                pindex = self.pindex_lookup.get(slug,-1)
                count = float(row["scroll_events"])
                if pindex > 0:
                    record = Pagedata(pindex=pindex,key="scroll_events",count=count,date=sql_timestr)
                    self.session.add(record)
            self.session.commit()
        if frame_type == "search":
            for idx,row in pv_df.iterrows():
                slug = row["slug"]
                if slug=="null":
                    continue
                pindex = self.pindex_lookup.get(slug,-1)
                count = float(row[field])
                if pindex > 0:
                    record = Pagedata(pindex=pindex,key=field,count=count,date=sql_timestr)
                    self.session.add(record)
            self.session.commit()
        if frame_type == "searchterm":
            for idx,row in pv_df.iterrows():
                sterm = row["slug"]
                if len(sterm) > 250: #hack to truncate long searches
                    sterm = sterm[0:250]
                count = row[field]
                record = Stermdata(sterm=sterm,key=field,count=count,date=sql_timestr)
                self.session.add(record)
            self.session.commit()
