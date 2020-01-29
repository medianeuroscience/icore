from flask import Flask, request, jsonify
from flask_cors import CORS, cross_origin

import os
import sys

import findspark
findspark.init()

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.11:2.3.0 --conf spark.cassandra.connection.host=127.0.0.1 pyspark-shell'
from pyspark import SparkContext

#sc = SparkContext("local", "article data app")
#from pyspark.sql import SQLContext
#sqlContext = SQLContext(sc)

app = Flask(__name__)

CORS(app)

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

@app.route('/', methods=['POST'])
def test():
    data = request.json
    year.append(data['year'])
    entity.append(data['entity'])
    month.append(data['month'])
    topic.append(data['topic'])
    if data['dictionary'] != 'default':
        dictionary.append(data['dictionary'])
    user_email.append(data['email'])
    if data['country'] != 'default':
        country.append(data['country'])
    keyword.append(data['keyword'])
    if data['issue'] != 'default':
        issue.append(data['issue'])
    meta.append(data['meta'])
    format.append(data['format'])
    return request.json


@app.route('/view', methods=['GET', 'POST'])
def view():

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

    #cassDf = sqlContext.read.format("org.apache.spark.sql.cassandra")\
    #.options(table= "angular_forms", keyspace = "scraped_articles")\
    #.load()\
    #.select('year', 'topics', 'entity', 'dict', 'english', 'month')

    #sqlDf = cassDf.registerTempTable('sqlTable')
    #cassDF_byTime = sqlContext.sql("""SELECT * FROM sqlTable WHERE year == '{}'""".format(str(year[0])))
    #cassDF_byTime.toPandas().to_csv('output_files.csv')

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


    return 'working' + ' ' + str(words)



if __name__ == '__main__':
     app.run(port=5000)
