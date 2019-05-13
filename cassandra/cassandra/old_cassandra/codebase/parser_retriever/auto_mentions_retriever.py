#Automated retrieval of gkg records from last 7 days

import urllib2
from mg_parse_gkg_cassandra import *

MASTERFILE_PATH = 'http://data.gdeltproject.org/gdeltv2/masterfilelist.txt'

import datetime

start_time = datetime.datetime.today().replace(hour=0,minute=0,second=0,microsecond=0)-timedelta(7)
start = start_time.strftime('%Y/%m/%d/%H/%M/%S').replace('/','')
end = datetime.datetime.today().replace(hour=0,minute=0,second=0,microsecond=0).strftime('%Y/%m/%d/%H/%M/%S').replace('/','')

interval_patterns = '0000' #set to 1h queries, see gkg_retriever.py for other options
                    
masterfile = urllib2.urlopen(MASTERFILE_PATH).readlines()

lines = [line.strip('\n').split() for line in masterfile]

infile_obj_list = []
for line in lines:
        try:
                filename = line[2].split('/')[-1]
        except IndexError:
                continue
        file_timestamp = filename.split('.')[0]
        if int(file_timestamp) < int(end) and int(file_timestamp)  > int(start) and [p for p in interval_patterns if file_timestamp.endswith(p)]:
                    gdelt_type = line[2].split('.')[-3]
                    if gdelt_type == 'mentions':
                            md5sum = line[1]
                            url = line[2]
                            infile_obj_list.append((gdelt_type, filename, url, md5sum))


try:
        do_parse_pool(infile_obj_list)
except KeyboardInterrupt:
        print('Interrupted retrieval process, aborting.')
