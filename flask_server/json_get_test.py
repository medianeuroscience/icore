from flask import Flask, request, jsonify   #backend server to communicate data
from flask_cors import CORS, cross_origin   #allow cross webpage resource sharing

import email, smtplib, ssl
#smtplib -- Simple Mail Transfer Protocol is a communication protocol for electronic mail transmission
#ssl -- Secure Sockets Layer and, in short, it's the standard technology for keeping an internet connection secure

#MIME module -- build complete message structures from scratch

import os
import sys

import findspark
findspark.init()

#initialize pyspark location on os

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.11:2.3.0 --conf spark.cassandra.connection.host=127.0.0.1 pyspark-shell'
from pyspark import SparkContext


sc = SparkContext("local", "article data app")
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

#create pyspark application and context

app = Flask(__name__)

CORS(app)

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
#build variables for storing and using user inputs


@app.route('/')
def test():
    return 'Hello, World!'

#extract relevant user inputs for passing into pyspark

@app.route('/data', methods=['GET', 'POST'])
def get():

    #pull data from cassandra table
    cassDf = sqlContext.read.format("org.apache.spark.sql.cassandra")\
    .options(table= "test_angcharts", keyspace = "scraped_articles")\
    .load()\
    .select('letter', 'frequency')

    sqlDf = cassDf.registerTempTable('sqlTable')
    cassDF_byTime = sqlContext.sql("""SELECT * FROM sqlTable""")

    #query through a sql context
    #cassDF_byTime.toPandas().to_csv('output_files.csv')
    json_query = cassDF_byTime.toPandas().to_json(orient="records", date_format="iso")

    return json_query


if __name__ == '__main__':
     app.run(port=5000, debug=True)
