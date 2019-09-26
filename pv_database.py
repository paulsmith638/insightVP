import sys,os,time
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy_utils import database_exists,create_database
from pv_ingest import Ingest
from pv_ingest import Utils
import unidecode as ud
#instantialize classes
ingest = Ingest()
util = Utils()


wordpress_database = "vp_wp_data"
mysql_login = "psmith"
mysql_pass = "Ins#ght2019"
mysql_port = 3306

output_database = "vinepair1"
sql_connect  =  'mysql://%s:%s@localhost:%s/%s' % (mysql_login,mysql_pass,mysql_port,wordpress_database)
read_engine = create_engine(sql_connect) 
#write_engine = create_engine('sqlite:///'+output_database)
sql_connect2  =  'mysql://%s:%s@localhost:%s/%s' % (mysql_login,mysql_pass,mysql_port,output_database)
write_engine = create_engine(sql_connect2)
if not database_exists(write_engine.url):
    create_database(write_engine.url)

write_con = write_engine.connect()

for table in ('pindex','tindex','ttype','pterms'):
    write_con.execute("DROP TABLE IF EXISTS %s" % table)
Base_write = declarative_base()
Base_read = automap_base()
Base_read.prepare(read_engine, reflect=True)
read_session = Session(read_engine)
write_session = Session(write_engine)
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
    termstr = Column(String(length=2048),default="")
    pindex = Column(Integer,index=True)
    #@property
    #def term_list(self):
    #    return list(elem.strip() for elem in self._termstr.split(','))
    #@term_list.setter
    #def term_list(self,tindex_toadd):
    #    if type(self._termstr) == type(None):
    #        self._termstr = ""
    #    self._termstr += ',%s' % str(tindex_toadd)
    def __repr__(self):
        return self.termstr
    
Base_read.metadata.create_all(read_engine)
Base_write.metadata.create_all(write_engine)

Pages= Base_read.classes.wp_po_plugins
pages = read_session.query(Pages)
for page in pages:
    slug = util.link2slug([page.permalink])
    index = page.post_id
    record = Slug(slug=slug,pindex=index)
    write_session.add(record)
write_session.commit()


Pages = Base_read.classes.wp_term_relationships
pages = read_session.query(Pages)
pterm_lookup = {}
for page in pages:
    pindex=page.object_id
    tindex=page.term_taxonomy_id
    pterm_list = pterm_lookup.get(pindex,[])
    pterm_list.append(tindex)
    pterm_lookup[pindex] = pterm_list

for pindex,tlist in pterm_lookup.iteritems():
    termstr = ",".join(str(term) for term in tlist)
    record=PageTerms(pindex=pindex,termstr=termstr)
    write_session.add(record)
write_session.commit()

Pages = Base_read.classes.wp_terms
pages = read_session.query(Pages)
for page in pages:
    tindex = page.term_id
    tname = page.name
    print tname
    record = Term(tindex=tindex,term=tname)
    write_session.add(record) 
write_session.commit()
Pages = Base_read.classes.wp_term_taxonomy
pages = read_session.query(Pages)
for page in pages:
    tindex = page.term_id
    ttype= page.taxonomy
    record = Type(tindex=tindex,ttype=ttype)
    write_session.add(record)
write_session.commit()
