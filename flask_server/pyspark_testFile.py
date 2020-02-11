from flask import Flask, request, jsonify   #backend server to communicate data
from flask_cors import CORS, cross_origin   #allow cross webpage resource sharing

import os
import sys
import time
import themesMapping
import gcamMapping

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


issues_dict = themesMapping.issues_dict
gcam_json = gcamMapping.gcam_json

gcam_vars = []
gcam_dims = []
gcam_dicts = []


for i in range(0, len(gcam_json["data"])):

  if gcam_json["data"][i]["DictionaryHumanName"] == "Moral Foundations Dictionary":
    gcam_vars.append(gcam_json["data"][i]["Variable"])
    gcam_dims.append((gcam_json["data"][i]["DimensionHumanName"]))
    gcam_dicts.append(gcam_json["data"][i]["DictionaryHumanName"])

  if gcam_json["data"][i]["DictionaryHumanName"] == "Linguistic Inquiry and Word Count (LIWC)":
    gcam_vars.append(gcam_json["data"][i]["Variable"])
    gcam_dims.append((gcam_json["data"][i]["DimensionHumanName"]))
    gcam_dicts.append(gcam_json["data"][i]["DictionaryHumanName"])

  if gcam_json["data"][i]["DictionaryHumanName"] == "Hogenraad's Motive Dictionary":
    gcam_vars.append(gcam_json["data"][i]["Variable"])
    gcam_dims.append((gcam_json["data"][i]["DimensionHumanName"]))
    gcam_dicts.append(gcam_json["data"][i]["DictionaryHumanName"])


#gcam_vars.insert(0, 'skip')
#gcam_dims.insert(0, 'skip')
#gcam_dicts.insert(0, 'skip')

#print(gcam_dicts)

#gcam_vars = gcam_vars[0:10]


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

    if data['issue'] == 'default':
        data['issue'] = 'issue'
        issue.append(data['issue'])
    else:
        issue.append(data['issue'])

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


    parameters = {'gkg_day': str(year[0]), 'source_location': str(country[0]), 'named_entities': str(entity[0]), 'gcam_data': str(dictionary[0]), 'themes': str(topic[0]),
                  'source': 'source', 'gkg_id': 'gkg_id'}

    user_gcamVars = []
    user_gcamDims = []

    for i in dictionary[0]:
        if i == 'mfd':
            for x, m in enumerate(gcam_dicts):
                if m == 'Moral Foundations Dictionary':
                    user_gcamVars.append(gcam_vars[x])
                    user_gcamDims.append(str(gcam_dims[x])+'_MFD')

        if i == 'liwc':
            for x, m in enumerate(gcam_dicts):
                if m == 'Linguistic Inquiry and Word Count (LIWC)':
                    user_gcamVars.append(gcam_vars[x])
                    user_gcamDims.append(str(gcam_dims[x]) + '_LIWC')

        if i == 'motive':
            for x, m in enumerate(gcam_dicts):
                if m == "Hogenraad's Motive Dictionary":
                    user_gcamVars.append(gcam_vars[x])
                    user_gcamDims.append(str(gcam_dims[x]) + '_HM')

    user_gcamVars.insert(0, 'skip')
    user_gcamDims.insert(0, 'skip')

    filtered_param = {k: v for (k, v) in parameters.items() if v != 'empty'}

    if str(issue[0]) != 'issue':
        themes_issue = issues_dict[str(issue[0]).lower()]

    user_entity = str(entity[0]).split(',')

    user_entity_proc = []

    for i in user_entity:
        i = i.strip()
        #i = i.lower()
        user_entity_proc.append(i)

    print(filtered_param)

    paras = ','.join("{}".format(k) for k, v in filtered_param.items() if v)

    paras = paras.replace("'", "")


    #print(user_gcamVars, user_gcamDims)

    #pull data from cassandra table
    cassDf = sqlContext.read.format("org.apache.spark.sql.cassandra")\
    .options(table= "gkg_record_by_day", keyspace = "icore_new")\
    .load()

    if len(month[0]) > 1:
        _start = str(year[0]) + '/'+ str(month[0]) + '/01'
        _end =  str(year[0]) + '/'+ str(month[0]) + '/05'

    time_range = pd.date_range(start=_start, end=_end)
    time_range= time_range.values.astype('<M8[D]').astype(str)

    #cassDf = cassDf.repartition(10)

    print(themes_issue)


    sqlDf = cassDf.registerTempTable('sqlTable')


    sqlDfList = []


    start_time = time.time()
    for t in time_range:
        cassDF_byTime = sqlContext.sql("""SELECT {} FROM sqlTable WHERE gkg_day = '{}'""".format(paras, t))

        if str(country[0]) != 'source_location':
            cassDF_byTime = cassDF_byTime.filter(cassDF_byTime.source_location == str(country[0]))
            #df2 = cassDF_byTime
            #df2 = df2.drop(cassDF_byTime.gcam_data)

        if str(dictionary[0]) != 'empty':
            df2 = cassDF_byTime.select([cassDF_byTime.gkg_id if i == 'skip' else cassDF_byTime.gcam_data.getItem(i).alias('{}'.format(user_gcamDims[user_gcamVars.index(i)])) for i in user_gcamVars])
            cassDF_byTime = cassDF_byTime.drop(cassDF_byTime.gcam_data)
            cassDF_byTime = df2.join(cassDF_byTime, on=['gkg_id'], how='inner')

        if str(entity[0]) != 'empty':
            df2 = cassDF_byTime.select(cassDF_byTime.gkg_id, explode(cassDF_byTime.named_entities).alias("entities2"))
            df2 = df2.filter(df2.entities2.isin(user_entity_proc))
            cassDF_byTime = df2.join(cassDF_byTime, on=['gkg_id'], how='inner').drop(df2.entities2)


        if str(topic[0]) != 'themes':
            df2 = cassDF_byTime.select(cassDF_byTime.gkg_id, explode(cassDF_byTime.themes).alias("themes2"))
            df2 = df2.filter(df2.themes2 == str(topic[0]).upper())
            cassDF_byTime = df2.join(cassDF_byTime, on=['gkg_id'], how='inner').drop(df2.themes2)

        if str(issue[0]) != 'issue':
            df2 = cassDF_byTime.select(cassDF_byTime.gkg_id, explode(cassDF_byTime.themes).alias("themes2"))
            df2 = df2.filter(df2.themes2.isin(themes_issue))
            cassDF_byTime = df2.join(cassDF_byTime, on=['gkg_id'], how='inner').drop(df2.themes2)



        cass_Pandas = cassDF_byTime.toPandas()
        sqlDfList.append(cass_Pandas)


    #query through a sql context

    print(" %s " % (time.time() - start_time))

    sqlDfList_output = pd.concat(sqlDfList)
    sqlDfList_output.to_csv('output_iter_files2.csv')

    print(len(sqlDfList_output))
    print(sqlDfList_output.head(5))
    print(sqlDfList_output.themes[0])



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
