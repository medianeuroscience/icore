#!/home/fhopp/anaconda2/bin/python

import sys, os
from cassandra.cqlengine.columns import *
from cassandra.cqlengine import connection
from cassandra.cqlengine.management import sync_table
from cassandra.cqlengine.models import Model
from cassandra.cluster import Cluster
import pandas as pd

os.environ['CQLENG_ALLOW_SCHEMA_MANAGEMENT'] = '1'

df = pd.read_csv('gtd17.csv')
del df['Unnamed: 0']
df = df.replace(' ', -99)
df = df.apply(pd.to_numeric, errors='ignore')
df = df.rename(columns={'INT_LOG':'int_log','INT_ANY':'int_any','INT_IDEO':'int_ideo',
                        'INT_MISC':'int_misc'})

CASSANDRA_CONTACT = '10.1.3.34'


class GTD(Model):
    eventid = Text()
    iyear= Integer(primary_key=True)
    imonth=Integer()
    iday=Integer()
    approxdate=Text()
    extended=Integer()
    resolution=Text()
    country=Integer(primary_key=True)
    country_txt= Text()
    region=Integer()
    region_txt= Text()
    provstate= Text()
    city=Text()
    latitude=Float()
    longitude=Float()
    specificity=Integer()
    vicinity=Integer()
    location= Text()
    summary= Text()
    crit1=Integer()
    crit2=Integer()
    crit3=Integer()
    doubtterr=Integer()
    alternative=Integer()
    alternative_txt= Text()
    multiple=Integer()
    success=Integer()
    suicide=Integer()
    attacktype1=Integer()
    attacktype1_txt= Text()
    attacktype2=Integer()
    attacktype2_txt= Text()
    attacktype3=Integer()
    attacktype3_txt= Text()
    targtype1=Integer()
    targtype1_txt= Text()
    targsubtype1=Integer()
    targsubtype1_txt= Text()
    corp1= Text()
    target1= Text()
    natlty1=Integer()
    natlty1_txt= Text()
    targtype2=Integer()
    targtype2_txt= Text()
    targsubtype2=Integer()
    targsubtype2_txt= Text()
    corp2= Text()
    target2= Text()
    natlty2=Integer()
    natlty2_txt= Text()
    targtype3=Integer()
    targtype3_txt= Text()
    targsubtype3=Integer()
    targsubtype3_txt= Text()
    corp3= Text()
    target3= Text()
    natlty3=Integer()
    natlty3_txt= Text()
    gname= Text()
    gsubname= Text()
    gname2= Text()
    gsubname2= Text()
    gname3= Text()
    gsubname3= Text()
    motive= Text()
    guncertain1=Integer()
    guncertain2=Integer()
    guncertain3=Integer()
    individual=Integer()
    nperps=Integer()
    nperpcap=Integer()
    claimed=Integer()
    claimmode=Integer()
    claimmode_txt= Text()
    claim2=Integer()
    claimmode2=Integer()
    claimmode2_txt=Text()
    claim3=Integer()
    claimmode3=Integer()
    claimmode3_txt=Text()
    compclaim=Integer()
    weaptype1= Text()
    weaptype1_txt= Text()
    weapsubtype1=Integer()
    weapsubtype1_txt= Text()
    weaptype2=Integer()
    weaptype2_txt= Text()
    weapsubtype2=Integer()
    weapsubtype2_txt= Text()
    weaptype3=Integer()
    weaptype3_txt= Text()
    weapsubtype3=Integer()
    weapsubtype3_txt= Text()
    weaptype4=Integer()
    weaptype4_txt= Text()
    weapsubtype4=Integer()
    weapsubtype4_txt= Text()
    weapdetail= Text()
    nkill=Integer()
    nkillus=Integer()
    nkillter=Integer()
    nwound=Integer()
    nwoundus=Integer()
    nwoundte=Integer()
    property=Integer()
    propextent=Integer()
    propextent_txt= Text()
    propvalue=Integer()
    propcomment= Text()
    ishostkid=Integer()
    nhostkid=Integer()
    nhostkidus=Integer()
    nhours=Integer()
    ndays=Integer()
    divert= Text()
    kidhijcountry= Text()
    ransom=Integer()
    ransomamt=Integer()
    ransomamtus=Integer()
    ransompaid=Integer()
    ransompaidus=Integer()
    ransomnote=Text()
    hostkidoutcome=Integer()
    hostkidoutcome_txt= Text()
    nreleased=Integer()
    addnotes= Text()
    scite1= Text()
    scite2= Text()
    scite3= Text()
    dbsource= Text()
    int_log=Integer()
    int_ideo=Integer()
    int_misc=Integer()
    int_any=Integer()
    related=Text()


def sync_tables(host=CASSANDRA_CONTACT):
    connection.setup([host], 'gtd', protocol_version=3)
    sync_table(GTD)

def establish_session(host=CASSANDRA_CONTACT):
    c = Cluster([host])
                #default_retry_policy=StubbornRetryPolicy())
    s = c.connect('gtd')
    return (c, s)


# def insert_gtd():
#     cluster, session = establish_session()
#     gtd_query = session.prepare(('INSERT INTO gtd ({}) VALUES ({});'.format(','.join([k for k in GTD._columns.keys()]),','.join(['?' for k in GTD._columns.keys()]))))
#
#
#     for i, row in df.iterrows():
#         gtd_dict = {}
#         for col in df.columns:
#             if type(row[col]) == str:
#                 gtd_dict[str(col)] = 'test'
#             else:
#                 gtd_dict[str(col)] = 99
#             #except:
#                 #print(col)
#         #print(gtd_dict)
#          #print(type(row[col]))
#         sys.stdout.write('.')
#         sys.stdout.flush()
#
#         session.execute(gtd_query, gtd_dict)
#
#     sys.stdout.write('Finished at {}.'.format(datetime.utcnow()))
#     sys.stdout.flush()
#     session.shutdown()
#     cluster.shutdown()
#
#     return True