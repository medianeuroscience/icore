from flask import Flask, request, jsonify   #backend server to communicate data
from flask_cors import CORS, cross_origin   #allow cross webpage resource sharing

import email, smtplib, ssl
#smtplib -- Simple Mail Transfer Protocol is a communication protocol for electronic mail transmission
#ssl -- Secure Sockets Layer and, in short, it's the standard technology for keeping an internet connection secure

from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

#MIME module -- build complete message structures from scratch

import os
import sys
import pandas as pd

import findspark
findspark.init()

#initialize pyspark location on os

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.11:2.3.0 --conf spark.cassandra.connection.host=127.0.0.1 pyspark-shell'
from pyspark import SparkContext
from pyspark import SparkConf

conf = SparkConf().set('spark.driver.host','127.0.0.1')
sc = SparkContext(master="local", appName="article data app", conf=conf)

from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

#create pyspark application and context

app = Flask(__name__)

CORS(app, resources={r"/api/*": {"origins": "*"}})

#initiate flask app

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


@app.route('/api/usereventsp', methods=['POST'])
def userEventsP():

    global year
    global month
    global event
    global user_email
    global country
    global keyword
    global format

    data = request.json
    year.append(data['year'])
    month.append(data['month'])
    if data['event'] != 'default':
        event.append(data['event'])
    user_email.append(data['email'])
    if data['country'] != 'default':
        country.append(data['country'])
    keyword.append(data['keyword'])
    format.append(data['format'])
    return request.json

#extract relevant user inputs for passing into pyspark


@app.route('/api/usersurveysp', methods=['POST'])
def userSurveysP():

    global survey_email
    global location
    global description
    global profession
    global institution

    data = request.json
    survey_email.append(data['email'])
    location.append(data['location'])
    description.append(data['description'])
    profession.append(data['profession'])
    institution.append(data['institution'])
    return request.json


@app.route('/api/usersurveysg', methods=['GET', 'POST'])
def userSurveysG():

    global survey_email
    global location
    global description
    global profession
    global institution



    words = {'col1': survey_email, 'col2': location, 'col3': description, 'col4': profession, 'col5': institution}
    user_df = pd.DataFrame(data=words)
    user_df.to_csv("userSurvey.csv")

    #location = []
    #institution = []
    #description = []
    #profession = []
    #survey_email = []

    return str(words)

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

@app.route('/api/usereventsg', methods=['GET', 'POST'])
def userEventsG():
    global year
    global month
    global event
    global user_email
    global country
    global keyword
    global format

    words = [year, month, event, user_email, country, keyword, format]

    year = []
    month = []
    event = []
    user_email = []
    country = []
    keyword = []
    format = []

    return 'working' + ' ' + str(words)


if __name__ == '__main__':
     app.run(port=5000)
