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
import numpy as np

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



    words = {'col1': survey_email, 'col2': location, 'col3': description}
    user_df = pd.DataFrame(data=words)
    user_df.to_csv("userSurvey.csv")

    #location = []
    #institution = []
    #description = []
    #profession = []
    #survey_email = []

    return str(survey_email)



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

    parameters = {"gkg_day": str(year[0]), "entity": str(entity[0]), "tone_avg": str(month[0]), "topic": str(topic[0]), "dictionary": str(dictionary[0]),
    "user_email": str(user_email[0]), "source_location": str(country[0]), "keyword": str(keyword[0]), "issue": str(issue[0]), "meta": str(meta[0]), "format": str(format[0])}

    filtered_param = {k:v for (k,v) in parameters.items() if len(v) > 1}

    url = ','.join("'{}'".format(v) for k,v in filtered_param.items() if v)
    print(url)

    #pull data from cassandra table
    cassDf = sqlContext.read.format("org.apache.spark.sql.cassandra")\
    .options(table= "gkg_query", keyspace = "scraped_articles")\
    .load()\
    .select('gkg_day', 'gcam', 'location', 'tone_avg', 'format')


    _start = str(year[0]) + '/'+ str(month[0]) + '/01'
    _end =  str(year[0]) + '/'+ str(month[0]) + '/03'

    time_range = pd.date_range(start=_start, end=_end)
    time_range= time_range.values.astype('<M8[D]').astype(str)


    sqlDf = cassDf.registerTempTable('sqlTable')


    sqlDfList = []

    for i in time_range:
        cassDF_byTime = sqlContext.sql("""SELECT tone_avg, format FROM sqlTable WHERE gkg_day == '{}'""".format(i))
        #cassDF_byTime.toPandas().to_csv(str(i)+'_files.csv')
        cass_Pandas = cassDF_byTime.toPandas()
        sqlDfList.append(cass_Pandas)

    #query through a sql context
    sqlDfList_output = pd.concat(sqlDfList).to_csv('output_iter_files.csv')


    #turn pyspark dataframe into pandas and stored as csv on local machine


    #set email configurations
    subject = "Your Query From iCoRe is Here!"             #to be changed
    body = "Please find attached your requested data."     #to be changed
    sender_email = "medianeuroscience.sb@gmail.com"        #to be changed
    receiver_email = str(user_email[0])
    password = "do100projects"             #to be secured by calling from stored os


    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = receiver_email
    message["Subject"] = subject

    #build a multipart message and and establish email headers

    message.attach(MIMEText(body, "plain"))
    #add body plain text to email

    filename = "output_iter_files.csv"
    #store email file in local folder

    #open a file in binary mode
    with open(filename, "rb") as attachment:
        # add file as application/octet-stream
        # email client can usually download this automatically as attachment
        part = MIMEBase("application", "octet-stream")
        part.set_payload(attachment.read())

    #encode file in ASCII characters to send by email
    #add header as key/value pair to attachment part
    encoders.encode_base64(part)

    part.add_header(
        "Content-Disposition",
        f"attachment; filename= {filename}",
    )

    #add attachment to message and convert message to string
    message.attach(part)
    text = message.as_string()

    #log in to server using secure context and send email
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, text)

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


@app.route('/api/usereventsg', methods=['GET', 'POST'])
def userEventsG():
    global year
    global month
    global event
    global user_email
    global country
    global keyword
    global format

    #pull data from cassandra table
    cassDf = sqlContext.read.format("org.apache.spark.sql.cassandra")\
    .options(table= "angular_forms", keyspace = "scraped_articles")\
    .load()\
    .select('year', 'topics', 'entity', 'dict', 'english', 'month')

    sqlDf = cassDf.registerTempTable('sqlTable')
    cassDF_byTime = sqlContext.sql("""SELECT * FROM sqlTable WHERE year == '{}'""".format(str(year[0])))

    #query through a sql context
    cassDF_byTime.toPandas().to_csv('output_files.csv')

    #turn pyspark dataframe into pandas and stored as csv on local machine


    #set email configurations
    subject = "Your Query From iCoRe is Here!"             #to be changed
    body = "Please find attached your requested data."     #to be changed
    sender_email = "medianeuroscience.sb@gmail.com"        #to be changed
    receiver_email = str(user_email[0])
    password = "do100projects"             #to be secured by calling from stored os


    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = receiver_email
    message["Subject"] = subject

    #build a multipart message and and establish email headers

    message.attach(MIMEText(body, "plain"))
    #add body plain text to email

    filename = "output_files.csv"
    #store email file in local folder

    #open a file in binary mode
    with open(filename, "rb") as attachment:
        # add file as application/octet-stream
        # email client can usually download this automatically as attachment
        part = MIMEBase("application", "octet-stream")
        part.set_payload(attachment.read())

    #encode file in ASCII characters to send by email
    #add header as key/value pair to attachment part
    encoders.encode_base64(part)

    part.add_header(
        "Content-Disposition",
        f"attachment; filename= {filename}",
    )

    #add attachment to message and convert message to string
    message.attach(part)
    text = message.as_string()

    #log in to server using secure context and send email
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, text)

    words = [year, month, event, user_email, country, keyword, format]

    year = []
    month = []
    event = []
    user_email = []
    country = []
    keyword = []
    format = []

    return 'working' + ' ' + str(words)

#extract relevant user inputs for passing into pyspark


if __name__ == '__main__':
     app.run(port=5000)
