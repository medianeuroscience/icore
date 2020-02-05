from flask import Flask, request, jsonify   #backend server to communicate data
from flask_cors import CORS, cross_origin   #allow cross webpage resource sharing


import os
import sys
import time

import pandas as pd
import numpy as np
import pyspark.sql.functions as f

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
        data['topic'] = 'empty'
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
        data['country'] = 'empty'
        country.append(data['country'])
    else:
        country.append(data['country'])

    if data['keyword'] == '':
        data['keyword'] = 'empty'
        keyword.append(data['keyword'])
    else:
        keyword.append(data['keyword'])

    if data['issue'] == 'default':
        data['issue'] = 'empty'
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

    parameters = {'gkg_day': str(year[0]), 'source_location': str(country[0]), 'source': str(entity[0]), 'gcam_data': str(dictionary[0])}

    filtered_param = {k: v for (k, v) in parameters.items() if v != 'empty'}

    paras = ','.join("{}".format(k) for k, v in filtered_param.items() if v)

    paras_input = ''.join("AND {} = '{}'".format(k,v) for k, v in filtered_param.items() if v)

    paras_list = paras.split(",")

    paras = paras.replace("'", "")


    #pull data from cassandra table
    cassDf = sqlContext.read.format("org.apache.spark.sql.cassandra")\
    .options(table= "gkg_record_by_day", keyspace = "icore_new")\
    .load()
   #.select('gkg_day', 'gcam', 'location', 'tone_avg', 'format')


    _start = str(year[0]) + '/'+ str(month[0]) + '/01'
    _end =  str(year[0]) + '/'+ str(month[0]) + '/03'

    time_range = pd.date_range(start=_start, end=_end)
    time_range= time_range.values.astype('<M8[D]').astype(str)


    sqlDf = cassDf.registerTempTable('sqlTable')


    sqlDfList = []

    for t in time_range:
        cassDF_byTime = sqlContext.sql("""SELECT {} FROM sqlTable WHERE gkg_day = '{}'""".format(paras, t))
        cassDF_byTime = cassDF_byTime.filter(cassDF_byTime.source_location == str(country[0]))
        #cassDF_byTime.toPandas().to_csv(str(i)+'_files.csv')

        cass_Pandas = cassDF_byTime.toPandas()
        sqlDfList.append(cass_Pandas)

    #query through a sql context
    sqlDfList_output = pd.concat(sqlDfList).to_csv('output_iter_files2.csv')


    words = [year, entity, month, topic, dictionary, user_email, country, keyword, issue, meta, format]

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
