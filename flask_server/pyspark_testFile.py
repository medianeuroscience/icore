from flask import Flask, request, jsonify   #backend server to communicate data
from flask_cors import CORS, cross_origin   #allow cross webpage resource sharing



import os
import sys
import time

import pandas as pd
import numpy as np
from pyspark.sql.functions import explode

#import findspark
#findspark.init()

#initialize pyspark location on os

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.11:2.3.0 --conf spark.cassandra.connection.host=10.1.3.115 pyspark-shell'
from pyspark import SparkContext
#from pyspark import SparkConf

#conf = SparkConf().set('spark.driver.host','127.0.0.1')
sc = SparkContext(master="local", appName="article data app")

from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

#create pyspark application and context

app = Flask(__name__)

CORS(app, resources={r"/api/*": {"origins": "*"}})

issues_dict = {"taxes" : ["ECON_TAXATION",],
"unemployment" : [ "UNEMPLOYMENT", ],
"domestic economy" : ["ECON_BANKRUPTCY", "ECON_BOYCOTT", "ECON_COST_OF_LIVING", "ECON_CUTOUTLOOK", "ECON_DEREGULATION", "ECON_EARNINGSREPORT", "ECON_ENTREPRENEURSHIP", "ECON_HOUSING_PRICES", "ECON_INFORMAL_ECONOMY", "ECON_IPO", "ECON_INTEREST_RATE", "ECON_MONOPOLY", "ECON_MOU", "ECON_NATIONALIZE", "ECON_PRICECONTROL", "ECON_REMITTANCE", "ECON_STOCKMARKET", "ECON_SUBSIDIES", "ECON_UNIONS", "SLFID_ECONOMIC_DEVELOPMENT", "SLFID_ECONOMIC_POWER", "SOC_ECONCOOP"],
"trade" : ["ECON_TRADE_DISPUTE", "ECON_FOREIGNINVEST", "ECON_FREETRADE", "ECON_CURRENCY_EXCHANGE_RATE", "ECON_CURRENCY_RESERVES", "ECON_DEBT"],
"terrorism" : ["TAX_TERROR_GROUP", "SUICIDE_ATTACK", "EXTREMISM", "JIHAD", "TERROR", "WMD"],
"military" : ["ACT_FORCEPOSTURE", "ARMEDCONFLICT", "BLOCKADE", "CEASEFIRE", "MILITARY", "MILITARY_COOPERATION", "PEACEKEEPING", "RELEASE_HOSTAGE", "SEIGE", "SLFID_MILITARY_BUILDUP", "SLFID_MILITARY_READINESS", "SLFID_MILITARY_SPENDING", "SLFID_PEACE_BUILDING", "TAX_MILITARY_TITLE"],
"international relations" : ["GOV_INTERGOVERNMENTAL", "SOC_DIPLOMCOOP", "RELATIONS"],
"immigration/refugees" : ["BORDER", "CHECKPOINT", "DISPLACED",  "EXILE", "IMMIGRATION", "REFUGEES", "SOC_FORCEDRELOCATION", "SOC_MASSMIGRATION", "UNREST_CHECKPOINT", "UNREST_CLOSINGBORDER"],
"health care" : ["GENERAL_HEALTH", "HEALTH_SEXTRANSDISEASE", "HEALTH_VACCINATION", "MEDICAL", "MEDICAL_SECURITY"],
"gun control" : ["FIREARM_OWNERSHIP", "MIL_SELF_IDENTIFIED_ARMS_DEAL", "MIL_WEAPONS_PROLIFERATION"],
"drug" : ["CRIME_ILLEGAL_DRUGS", "DRUG_TRADE", "TAX_CARTELS", "CRIME_CARTELS"],
"police system" : ["UNREST_POLICEBRUTALITY", "SECURITY_SERVICES"],
"racism" : ["DISCRIMINATION", "HATE_SPEECH"],
"civil liberties" : ["GENDER_VIOLENCE", "LGBT", "MOVEMENT_SOCIAL",  "MOVEMENT_WOMENS", "SLFID_CIVIL_LIBERTIES"],
"environment" :  ["ENV_BIOFUEL", "ENV_CARBONCAPTURE", "ENV_CLIMATECHANGE",  "ENV_COAL", "ENV_DEFORESTATION", "ENV_FISHERY", "ENV_FORESTRY", "ENV_GEOTHERMAL", "ENV_GREEN", "ENV_HYDRO", "ENV_METALS", "ENV_MINING", "ENV_NATURALGAS", "ENV_NUCLEARPOWER", "ENV_OIL", "ENV_OVERFISH", "ENV_POACHING", "ENV_WATERWAYS ", "ENV_SOLAR", "ENV_SPECIESENDANGERED", "ENV_SPECIESEXTINCT", "ENV_WINDPOWER", "FUELPRICES", "MOVEMENT_ENVIRONMENTAL", "SELF_IDENTIFIED_ENVIRON_DISASTER", "SLFID_MINERAL_RESOURCES", "SLFID_NATURAL_RESOURCES", "WATER_SECURITY"],
"party-politics" : ["TAX_POLITICAL_PARTY"],
"election fraud" : ["ELECTION_FRAUD"],
"education" : ["EDUCATION"],
"media/internet" : ["CYBER_ATTACK",  "INTERNET_BLACKOUT", "INTERNET_CENSORSHIP", "MEDIA_CENSORSHIP", "MEDIA_MSM", "MEDIA_SOCIAL", "SURVEILLANCE", "FREESPEECH"],
}

#initiate flask app

year = []
entity = []
month = []
topic = []
dictionary = []
user_email = []
country = []
keyword = []
issue = []
meta = []
format = []
event = []
#build variables for storing and using user inputs
location = []
institution = []
description = []
profession = []
survey_email = []

@app.route('/', methods=['GET', 'POST'])
def test():
    return 'Hello';

@app.route('/api/usergkgp', methods=['POST'])
def userGkgP():

    global year
    global entity
    global month
    global topic
    global dictionary
    global user_email
    global country
    global keyword
    global issue
    global meta
    global format

    data = request.json

    if data['year'] == '':
        data['year'] = 'empty'
        year.append(data['year'])
    else:
        year.append(data['year'])

    if data['entity'] == '':
        data['entity'] = 'empty'
        entity.append(data['entity'])
    else:
        entity.append(data['entity'])

    if data['month'] == '':
        data['month'] = 'empty'
        month.append(data['month'])
    else:
        month.append(data['month'])

    if data['topic'] == 'default':
        data['topic'] = 'themes'
        topic.append(data['topic'])
    else:
        topic.append(data['topic'])

    if data['dictionary'] == '':
        data['dictionary'] = 'empty'
        dictionary.append(data['dictionary'])
    else:
        dictionary.append(data['dictionary'])

    if data['email'] == '':
        data['email'] = 'empty'
        user_email.append(data['email'])
    else:
        user_email.append(data['email'])

    if data['country'] == 'default':
        data['country'] = 'source_location'
        country.append(data['country'])
    else:
        country.append(data['country'])

    if data['keyword'] == '':
        data['keyword'] = 'empty'
        keyword.append(data['keyword'])
    else:
        keyword.append(data['keyword'])

    if data['issue'] == 'default':
        data['issue'] = 'issue'
        issue.append(data['issue'])
    else:
        issue.append(data['issue'])

    if data['meta'] == '':
        data['meta'] = 'empty'
        meta.append(data['meta'])
    else:
        meta.append(data['meta'])

    if data['format'] == '':
        data['format'] = 'empty'
        format.append(data['format'])
    else:
        format.append(data['format'])

    return request.json



@app.route('/api/usergkgg', methods=['GET', 'POST'])
def userGkgG():

    global year
    global entity
    global month
    global topic
    global dictionary
    global user_email
    global country
    global keyword
    global issue
    global meta
    global format

    time.sleep(1)


    parameters = {'gkg_day': str(year[0]), 'source_location': str(country[0]), 'named_entities': str(entity[0]), 'gcam_data': str(dictionary[0]), 'themes': str(topic[0]).upper(),
                  'source': 'source', 'gkg_id': 'gkg_id'}

    filtered_param = {k: v for (k, v) in parameters.items() if v != 'empty'}

    if str(issue[0]) != 'issue':
        themes_issue = issues_dict[str(issue[0]).lower()]

    paras = ','.join("{}".format(k) for k, v in filtered_param.items() if v)

    paras_input = ''.join("AND {} = '{}'".format(k,v) for k, v in filtered_param.items() if v)

    paras_list = paras.split(",")

    paras = paras.replace("'", "")


    #pull data from cassandra table
    cassDf = sqlContext.read.format("org.apache.spark.sql.cassandra")\
    .options(table= "gkg_record_by_day", keyspace = "icore_new")\
    .load()

    if len(month[0]) > 1:
        _start = str(year[0]) + '/'+ str(month[0]) + '/01'
        _end =  str(year[0]) + '/'+ str(month[0]) + '/30'

    time_range = pd.date_range(start=_start, end=_end)
    time_range= time_range.values.astype('<M8[D]').astype(str)

    #cassDf = cassDf.repartition(10)


    sqlDf = cassDf.registerTempTable('sqlTable')


    sqlDfList = []

    start_time = time.time()
    for t in time_range:
        cassDF_byTime = sqlContext.sql("""SELECT {} FROM sqlTable WHERE gkg_day = '{}'""".format(paras, t))

        if str(country[0]) != 'source_location':
            cassDF_byTime = cassDF_byTime.filter(cassDF_byTime.source_location == str(country[0]))

        if str(topic[0]) != 'themes':
            df2 = cassDF_byTime.select(cassDF_byTime.gkg_id, explode(cassDF_byTime.themes).alias("themes2"))
            df2 = df2.filter(df2.themes2 == str(topic[0]).upper())
            df2 = df2.join(cassDF_byTime, on=['gkg_id'], how='inner').drop(df2.themes2)

        if str(issue[0]) != 'issue':
            df2 = cassDF_byTime.select(cassDF_byTime.gkg_id, explode(cassDF_byTime.themes).alias("themes2"))
            df2 = df2.filter(df2.themes2.isin(themes_issue))
            df2 = df2.join(cassDF_byTime, on=['gkg_id'], how='inner').drop(df2.themes2)

        cass_Pandas = df2.toPandas()
        sqlDfList.append(cass_Pandas)

    #query through a sql context

    print(" %s " % (time.time() - start_time))

    sqlDfList_output = pd.concat(sqlDfList)
    sqlDfList_output.to_csv('output_iter_files2.csv')

    print(len(sqlDfList_output))
    print(sqlDfList_output.tail(5))
    print(themes_issue)


    #print(cassDF_byTime.rdd.getNumPartitions())


    #words = [year, entity, month, topic, dictionary, user_email, country, keyword, issue, meta, format]

    year = []
    entity = []
    month = []
    topic = []
    dictionary = []
    user_email = []
    country = []
    keyword = []
    issue = []
    meta = []
    format = []

    return 'working'


if __name__ == '__main__':
     app.run(port=5000, host='0.0.0.0')
