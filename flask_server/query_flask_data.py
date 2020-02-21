from .themes import themesMapping
from .gcam import gcamMapping

import sys
# sys.path.append('/local/icore/anaconda3/envs/wsgiFlask/lib/python3.6/site-packages')

from flask import Flask, request, jsonify  # backend server to communicate data
from flask_cors import CORS, cross_origin  # allow cross webpage resource sharing
from flask import request

import email, smtplib, ssl
# smtplib -- Simple Mail Transfer Protocol is a communication protocol for electronic mail transmission
# ssl -- Secure Sockets Layer and, in short, it's the standard technology for keeping an internet connection secure

from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# MIME module -- build complete message structures from scratch

import os
import time

from pyspark.sql.functions import explode
import pandas as pd

# initialize pyspark configs
os.environ[
    'PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.11:2.3.0 --conf spark.cassandra.connection.host=10.1.3.115 pyspark-shell'
from pyspark import SparkContext

sc = SparkContext(master="local[12]", appName="iCoRe App")

from pyspark.sql import SQLContext

sqlContext = SQLContext(sc)

# create pyspark application and context


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

# initiate flask app


year = []
entity = []
month = []
topic = []
dictionary = []
user_email = []
country = []
issue = []
format = []
event = []
# build variables for storing and using user inputs
location = []
institution = []
description = []
profession = []
survey_email = []


@app.route('/', methods=['GET', 'POST'])
def test():
    return 'Hello from iCoRe API service';


@app.route('/usergkgp', methods=['POST'])
def userGkgP():
    global year
    global entity
    global month
    global topic
    global dictionary
    global user_email
    global country
    global issue
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


@app.route('/usereventsp', methods=['POST'])
def userEventsP():
    global year
    global month
    global event
    global user_email
    global country
    global format

    data = request.json

    if data['year'] == '':
        data['year'] = 'empty'
        year.append(data['year'])
    else:
        year.append(data['year'])

    if data['month'] == '':
        data['month'] = 'empty'
        month.append(data['month'])
    else:
        month.append(data['month'])

    if data['country'] == 'default':
        data['country'] = 'source_location'
        country.append(data['country'])
    else:
        country.append(data['country'])

    if data['email'] == '':
        data['email'] = 'empty'
        user_email.append(data['email'])
    else:
        user_email.append(data['email'])

    if data['event'] == 'default':
        data['event'] = 'event'
        event.append(data['event'])
    else:
        event.append(data['event'])

    if data['format'] == '':
        data['format'] = 'empty'
        format.append(data['format'])
    else:
        format.append(data['format'])

    return request.json


@app.route('/usersurveysp', methods=['POST'])
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


@app.route('/usersurveysg', methods=['GET', 'POST'])
def userSurveysG():

    global survey_email
    global location
    global description
    global profession
    global institution

    survey_record = {'user_email': survey_email, 'user_location': location, 'user_description': description,
                     'user_profession': profession, 'user_institution': institution}
    survey_df = pd.DataFrame(data=survey_record)
    survey_df.to_csv("/var/www/mm_icore/flask_server/survey_responses/{}.csv".format(survey_email))


    location = []
    institution = []
    description = []
    profession = []
    survey_email = []

    return 'working' + str(survey_record)


@app.route('/usergkgg', methods=['GET', 'POST'])
def userGkgG():
    global year
    global entity
    global month
    global topic
    global dictionary
    global user_email
    global country
    global issue
    global format

    time.sleep(1)

    # sc = SparkContext(master="local[12]", appName="GKGApp")
    # sqlContext = SQLContext(sc)

    parameters = {'gkg_day': str(year[-1]), 'source_location': str(country[-1]), 'named_entities': str(entity[-1]),
                  'gcam_data': str(dictionary[-1]), 'themes': str(topic[-1]),
                  'source': 'source', 'gkg_id': 'gkg_id'}

    user_gcamVars = []
    user_gcamDims = []

    for i in dictionary[-1]:
        if i == 'mfd':
            for x, m in enumerate(gcam_dicts):
                if m == 'Moral Foundations Dictionary':
                    user_gcamVars.append(gcam_vars[x])
                    user_gcamDims.append(str(gcam_dims[x]) + '_MFD')

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

    if str(issue[-1]) != 'issue':
        themes_issue = issues_dict[str(issue[-1]).lower()]

    user_entity = str(entity[-1]).split(',')

    user_entity_proc = []

    for i in user_entity:
        i = i.strip()
        # i = i.lower()
        user_entity_proc.append(i)

    paras = ','.join("{}".format(k) for k, v in filtered_param.items() if v)
    paras = paras.replace("'", "")

    # pull data from cassandra table
    cassDf = sqlContext.read.format("org.apache.spark.sql.cassandra") \
        .options(table="gkg_record_by_day", keyspace="icore_new") \
        .load()

    if len(month[-1]) == 2:
        _start = str(year[-1]) + '/' + str(month[-1]) + '/01'
        _end = str(year[-1]) + '/' + str(month[-1]) + '/30'
        time_range = pd.date_range(start=_start, end=_end)
        time_range = time_range.values.astype('<M8[D]').astype(str)

    elif len(month[-1]) == 5:
        _start = str(year[-1]) + '/01/01'
        _end = str(year[-1]) + '/12/30'
        time_range = pd.date_range(start=_start, end=_end)
        time_range = time_range.values.astype('<M8[D]').astype(str)

    sqlDf = cassDf.registerTempTable('sqlTable')

    sqlDfList = []

    start_time = time.time()
    for t in time_range:
        cassDF_byTime = sqlContext.sql("""SELECT {} FROM sqlTable WHERE gkg_day = '{}'""".format(paras, t))

        if str(country[-1]) != 'source_location':
            cassDF_byTime = cassDF_byTime.filter(cassDF_byTime.source_location == str(country[-1]))

        if str(dictionary[-1]) != 'empty':
            df2 = cassDF_byTime.select([cassDF_byTime.gkg_id if i == 'skip' else cassDF_byTime.gcam_data.getItem(
                i).alias('{}'.format(user_gcamDims[user_gcamVars.index(i)])) for i in user_gcamVars])
            cassDF_byTime = cassDF_byTime.drop(cassDF_byTime.gcam_data)
            cassDF_byTime = df2.join(cassDF_byTime, on=['gkg_id'], how='inner')

        if str(entity[-1]) != 'empty':
            df2 = cassDF_byTime.select(cassDF_byTime.gkg_id, explode(cassDF_byTime.named_entities).alias("entities2"))
            df2 = df2.filter(df2.entities2.isin(user_entity_proc))
            cassDF_byTime = df2.join(cassDF_byTime, on=['gkg_id'], how='inner').drop(df2.entities2)

        if str(topic[-1]) != 'themes':
            df2 = cassDF_byTime.select(cassDF_byTime.gkg_id, explode(cassDF_byTime.themes).alias("themes2"))
            df2 = df2.filter(df2.themes2 == str(topic[-1]).upper())
            cassDF_byTime = df2.join(cassDF_byTime, on=['gkg_id'], how='inner').drop(df2.themes2)

        if str(issue[-1]) != 'issue':
            df2 = cassDF_byTime.select(cassDF_byTime.gkg_id, explode(cassDF_byTime.themes).alias("themes2"))
            df2 = df2.filter(df2.themes2.isin(themes_issue))
            cassDF_byTime = df2.join(cassDF_byTime, on=['gkg_id'], how='inner').drop(df2.themes2)

        cass_Pandas = cassDF_byTime.toPandas()
        sqlDfList.append(cass_Pandas)

    # query through a sql context

    # print(" %s " % (time.time() - start_time))

    sqlDfList_output = pd.concat(sqlDfList)
    # print(len(sqlDfList_output))

    if str(format[-1]) == "json":
        sqlDfList_output.to_json(
            '/var/www/mm_icore/flask_server/user_gkg_data/{}_query.json'.format(str(user_email[-1])), orient='records')

    else:
        sqlDfList_output.to_csv('/var/www/mm_icore/flask_server/user_gkg_data/{}_query.csv'.format(str(user_email[-1])))

    # print(len(sqlDfList_output))
    # print(sqlDfList_output.head(5))

    # turn pyspark dataframe into pandas and stored as csv on local machine

    # set email configurations
    subject = "Your Query From iCoRe is Here!"  # to be changed
    body = """<pre> Please find attached your requested data. Go to the page: <a href="https://www.google.com/">click here</a>Thanks, MNL Team.</pre>""" # to be changed
    sender_email = "medianeuroscience.sb@gmail.com"  # to be changed
    receiver_email = str(user_email[-1])
    password = "do100projects"  # to be secured by calling from stored os

    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = receiver_email
    message["Subject"] = subject

    # build a multipart message and and establish email headers


    message.attach(MIMEText(body, "html"))
    # add body plain text to email

    if str(format[-1]) == "json":
        filename = '/var/www/mm_icore/flask_server/user_gkg_data/{}_query.json'.format(str(user_email[-1]))

    else:
        filename = '/var/www/mm_icore/flask_server/user_gkg_data/{}_query.csv'.format(str(user_email[-1]))

    # filename = "output_files.csv"
    # store email file in local folder

    # open a file in binary mode
    with open(filename, "rb") as attachment:
        # add file as application/octet-stream
        # email client can usually download this automatically as attachment
        part = MIMEBase("application", "octet-stream")
        part.set_payload(attachment.read())

    # encode file in ASCII characters to send by email
    # add header as key/value pair to attachment part
    encoders.encode_base64(part)

    part.add_header(
        "Content-Disposition",
        "attachment; filename={}".format(os.path.basename(filename)),
    )

    # add attachment to message and convert message to string
    message.attach(part)
    text = message.as_string()

    # log in to server using secure context and send email
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, text)

    year = []
    entity = []
    month = []
    topic = []
    dictionary = []
    user_email = []
    country = []
    issue = []
    format = []

    # sc.stop()

    return 'working' + ' '


@app.route('/usereventsg', methods=['GET', 'POST'])
def userEventsG():
    global year
    global month
    global event
    global user_email
    global country
    global format

    time.sleep(1)

    # sc = SparkContext(master="local", appName="event app")
    # sqlContext = SQLContext(sc)

    parameters = {'event_day': str(year[0]), 'event_id': 'event_id', 'event_tone_avg': 'event_tone_avg',
                  'num_articles': 'num_articles', 'num_mentions': 'num_mentions', 'num_sources': 'num_sources',
                  'action_geo_name': 'action_geo_name'}

    filtered_param = {k: v for (k, v) in parameters.items() if v != 'empty'}

    paras = ','.join("{}".format(k) for k, v in filtered_param.items() if v)
    paras = paras.replace("'", "")

    # pull data from cassandra table
    cassDf = sqlContext.read.format("org.apache.spark.sql.cassandra") \
        .options(table="event_by_day", keyspace="icore_new") \
        .load()

    if len(month[0]) == 2:
        _start = str(year[0]) + '/' + str(month[0]) + '/01'
        _end = str(year[0]) + '/' + str(month[0]) + '/30'
        time_range = pd.date_range(start=_start, end=_end)
        time_range = time_range.values.astype('<M8[D]').astype(str)

    elif len(month[0]) == 5:
        _start = str(year[0]) + '/01/01'
        _end = str(year[0]) + '/12/30'
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

    if str(format[0]) == "json":
        sqlDfList_output.to_json(
            '/var/www/mm_icore/flask_server/user_event_data/{}_query.json'.format(str(user_email[0])), orient='records')

    else:
        sqlDfList_output.to_csv(
            '/var/www/mm_icore/flask_server/user_event_data/{}_query.csv'.format(str(user_email[0])))

    # turn pyspark dataframe into pandas and stored as csv on local machine

    # set email configurations
    subject = "Your Query From iCoRe is Here!"  # to be changed
    body = "Please find attached your requested data."  # to be changed
    sender_email = "medianeuroscience.sb@gmail.com"  # to be changed
    receiver_email = str(user_email[0])
    password = "do100projects"  # to be secured by calling from stored os

    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = receiver_email
    message["Subject"] = subject

    # build a multipart message and and establish email headers

    message.attach(MIMEText(body, "plain"))
    # add body plain text to email

    if str(format[0]) == "json":
        filename = '/var/www/mm_icore/flask_server/user_event_data/{}_query.json'.format(str(user_email[0]))

    else:
        filename = '/var/www/mm_icore/flask_server/user_event_data/{}_query.csv'.format(str(user_email[0]))

    # open a file in binary mode
    with open(filename, "rb") as attachment:
        # add file as application/octet-stream
        # email client can usually download this automatically as attachment
        part = MIMEBase("application", "octet-stream")
        part.set_payload(attachment.read())

    # encode file in ASCII characters to send by email
    # add header as key/value pair to attachment part
    encoders.encode_base64(part)

    part.add_header(
        "Content-Disposition",
        "attachment; filename={}".format(os.path.basename(filename)),
    )

    # add attachment to message and convert message to string
    message.attach(part)
    text = message.as_string()

    # log in to server using secure context and send email
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, text)

    year = []
    month = []
    event = []
    user_email = []
    country = []
    format = []

    return 'working' + ' '


# extract relevant user inputs for passing into pyspark


if __name__ == '__main__':
    app.run()
