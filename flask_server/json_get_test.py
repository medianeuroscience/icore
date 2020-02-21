from flask import Flask, request, jsonify   #backend server to communicate data
from flask_cors import CORS, cross_origin   #allow cross webpage resource sharing


import os
import sys
import time
import pandas as pd

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.11:2.3.0 --conf spark.cassandra.connection.host=10.1.3.115 pyspark-shell'
from pyspark import SparkContext
from pyspark.sql import SQLContext



#create pyspark application and context
app = Flask(__name__)

CORS(app, resources={r"/api/*": {"origins": "*"}})

#initiate flask app



@app.route('/')
def test():
    return 'Hello, World!'

#extract relevant user inputs for passing into pyspark

@app.route('/api/eventsDash', methods=['GET', 'POST'])
def get():

    sc = SparkContext("local", "eventsDash app")
    sqlContext = SQLContext(sc)

    parameters = {'event_day': 'event_day', 'event_id': 'event_id', 'event_tone_avg': 'event_tone_avg',
                  'num_articles': 'num_articles', 'num_mentions': 'num_mentions', 'num_sources': 'num_sources',
                  'action_geo_name': 'action_geo_name', 'action_geo_lat':  'action_geo_lat', 'action_geo_long':'action_geo_long', 'event_root_code': 'event_root_code'}

    filtered_param = {k: v for (k, v) in parameters.items() if v != 'empty'}

    paras = ','.join("{}".format(k) for k, v in filtered_param.items() if v)
    paras = paras.replace("'", "")


    # pull data from cassandra table
    cassDf = sqlContext.read.format("org.apache.spark.sql.cassandra") \
        .options(table="event_by_day", keyspace="icore_new") \
        .load()

    _start = '2015/09/01'
    _end = '2015/09/30'
    time_range = pd.date_range(start=_start, end=_end)
    time_range = time_range.values.astype('<M8[D]').astype(str)

    sqlDf = cassDf.registerTempTable('sqlTable')

    sqlDfList = []

    start_time = time.time()
    for t in time_range:
        cassDF_byTime = sqlContext.sql("""SELECT {} FROM sqlTable WHERE event_day = '{}'""".format(paras, t))

        cass_Pandas = cassDF_byTime.toPandas()
        sqlDfList.append(cass_Pandas)

    # query through a sql context

    print(" %s " % (time.time() - start_time))

    sqlDfList_output = pd.concat(sqlDfList)

    print(len(sqlDfList_output))
    print(sqlDfList_output.head(5))


    csv_query = sqlDfList_output.to_csv()

    sc.stop()

    #json_query = cassDF_byTime.toPandas().to_json(orient="records", date_format="iso")

    return csv_query


if __name__ == '__main__':
     app.run(port=5000, host='0.0.0.0')
