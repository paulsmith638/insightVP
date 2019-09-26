import sys,os,time
import mysql.connector
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import Column, Integer, String
from sqlalchemy_utils import database_exists, create_database
from pv_ingest import Ingest
from pv_ingest import Utils
#instantialize classes
ingest = Ingest()
util = Utils()




#app.config['SQLALCHEMY_DATABASE_URI']='mysql:////vinepair:VinePair2019@localhost:3306/vinepair1?auth_plugin=mysql_native_password'
read_engine = create_engine('mysql://psmith:Ins#ght2019@localhost:3306/vp_wp_data')
#write_engine = create_engine('sqlite:///test_db.sqlite')
write_engine = create_engine('mysql://psmith:Ins#ght2019@localhost:3306/vinepair1')
if not database_exists(write_engine.url):
    create_database(write_engine.url)

write_con = write_engine.connect()

for table in (
write_con.execute("")
Base = automap_base()
Base.prepare(read_engine, reflect=True)

session = Session(read_engine)
Pages= Base.classes.wp_po_plugins
pages = session.query(Pages)
pindex_lookup = {}
for page in pages:
    slug = util.link2slug([page.permalink])
    index = page.post_id
    pindex_lookup[slug] = index

Pages = Base.classes.wp_term_relationships
pages = session.query(Pages)
pterm_lookup = {}
for page in pages:
    pindex=page.object_id
    tindex=page.term_taxonomy_id
    pterm_list = pterm_lookup.get(pindex,[])
    pterm_list.append(tindex)
    pterm_lookup[pindex] = pterm_list

Pages = Base.classes.wp_terms
pages = session.query(Pages)
tname_lookup = {}
for page in pages:
    tindex = page.term_id
    tname = page.name
    tname_lookup[tindex] = tname

Pages = Base.classes.wp_term_taxonomy
pages = session.query(Pages)
ttype_lookup = {}
for page in pages:
    tindex = page.term_id
    ttype= page.taxonomy
    tname_lookup[tindex] = ttype

for lookup in (pindex_lookup,pterm_lookup,tname_lookup,ttype_lookup):
    for k,v in lookup.iteritems():
        print k,v
sys.exit()

#tables
#pindex is lookup for stripped slug to page index
#pterms is lookup for pindex to terms
#tindex is lookup for term index to term name
#ttype is lookup for term index to term type
"""
vp_tables = {"pindex":{"columns":["pslug","pindex"],"dtypes":["varchar(1000)","bigint"],
                       "source_table":"wp_po_plugins","source_cols":["permalink","post_id"]},
             "pterms":{"columns":["pindex","pterm"],"dtypes":["bigint","bigint"]},
             "tindex":{"columns":["tindex","tname"],"dtypes":["bigint","varchar(100)"]},
             "ttype":{"columns":["tindex","ttype"],"dtypes":["bigint","varchar(100)"]}}

mycursor.execute("use vinepair1")

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



for table,params in vp_tables.iteritems(): #probably not injection safe?
    mycursor.execute("DROP TABLE IF EXISTS %s" % table)
    col_names = params["columns"]
    col_types = params["dtypes"]
    create_ins = "id INT AUTO_INCREMENT PRIMARY KEY,"+",".join("%s %s" % (t[0],t[1]) for t in zip(col_names,col_types))
    mycursor.execute("CREATE TABLE %s ( %s )" % (table,create_ins))
    if table == "pindex":
        for sind,source in enumerate(params["source_cols"]):
            if source == "permalink":
                qdat = mycursor.execute("SELECT %s FROM %s.%s" % (source,wp_data,params["source_table"]))
                cdat = list(link2slug(link) for link in mycursor)
            if source == "post_id":
                qdat = mycursor.execute("SELECT %s FROM %s.%s" % (source,wp_data,params["source_table"]))
                cdat = list(str(idn[0]) for idn in mycursor)

            df=pd.DataFrame(cdat)
            df.to_sql(con=engine, name=table, if_exists='replace')
            #mycursor.execute("COMMIT")
        #dest_str =  "INSERT INTO vinepair1.%s (%s)" % (table,",".join(col for col in params["columns"]))
        #source_str = "SELECT %s FROM %s.%s" % (",".join(params["source_cols"]),wp_data,params["source_table"])
        #print dest_str,source_str
        #mycursor.execute("INSERT INTO vinepair1.%s (%s)" % (table,",".join(col for col in params["columns"])))
        #mycursos.execute("SELECT %s FROM %s.%s" % (",".join(params["source_cols"]),wp_data,params["source_table"]));

    

    
    #mycursor.execute("DESC %s" % table)
#  `id` int(11) NOT NULL AUTO_INCREMENT,
#  `publisher_id` varchar(1000) COLLATE utf8mb4_unicode_520_ci NOT NULL,
#  `api_key` varchar(1000) COLLATE utf8mb4_unicode_520_ci NOT NULL,
#  `status` tinyint(1) NOT NULL,
#  UNIQUE KEY `id` (`id`)
#) 
    
"""
