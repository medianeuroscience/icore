#!/usr/bin/python

import zipfile, csv, sys, mimetypes, urlparse, urllib2, os, re
from collections import defaultdict
from hashlib import md5
from cStringIO import StringIO
from multiprocessing import Pool
from datetime import datetime
from dateutil.parser import parse
from cassandra.cqlengine.columns import *
from cassandra.cqlengine import connection
from cassandra.cqlengine.management import sync_table
from cassandra.cqlengine.models import Model
from cassandra.cluster import Cluster
from cassandra.policies import RetryPolicy

# need to run mg_init_db_cassandra.py to make sure this exists!
#from gcam_value_model import GcamValue

os.environ['CQLENG_ALLOW_SCHEMA_MANAGEMENT'] = '1'
whitelist = defaultdict(list)
with open('whitelist.csv','r') as infile:
    r = csv.DictReader(infile)
    for ln in r:
        for k,v in ln.items():
            whitelist[k].append(v)

CASSANDRA_CONTACT = '10.1.3.79' # fhopp: new contact?
EXPECTED_COLUMNS = {
    'gkg':27,
    'mentions':16,
    'event':61,
}

class InputFileByUrl(Model):
    file_url = Text(primary_key=True)
    file_timestamp = DateTime(primary_key=True,clustering_order='DESC',index=True)
    file_timestamp.truncate_microseconds = True
    file_type = Text()
    file_name = Text()
    start_timestamp = DateTime(default=datetime.utcnow)
    start_timestamp.truncate_microseconds=True
    finish_timestamp = DateTime()
    finish_timestamp.truncate_microseconds=True
    included_rows = Integer() 
    excluded_rows = Integer()

class GkgIdByParent(Model):
    parent_input = Text(primary_key=True)
    gkg_id = Text(primary_key=True)

class GkgIdByUrl(Model):
    url = Text(primary_key=True)
    gkg_id = Text(primary_key=True)

class GkgRecordByDay(Model):
    url = Text(primary_key=True) 
    gkg_id = Text(index=True)
    gkg_day = DateTime(partition_key=True)
    gkg_day.truncate_microseconds=True
    gkg_timestamp = DateTime()
    gkg_timestamp.truncate_microseconds=True
    source = Text(index=True)
    source_location = Text(index=True)
    parent_input = Text()
    gcam_data = Map(key_type=Text,value_type=Text)
    mft_data = Map(key_type=Text,value_type=Text)
    tone_avg = Float()
    tone_pos = Float()
    tone_neg = Float()
    tone_polarity = Float()
    tone_act_ref_density = Float()
    tone_selfgrp_ref_density = Float()
    wordcount = Integer()
    event_ids = Set(value_type=Text,index=True)
    event_themes = Set(value_type=Text,index=True)
    event_locations = Set(value_type=Text,index=True)
    #event_organizations = Set(value_type=Text, index=True)
    event_actors = Set(value_type=Text, index=True)
    named_entities = Set(value_type=Text,index=True)
    themes = Set(value_type=Text,index=True)    
    
class GkgRecordByDate(Model):
    url = Text(primary_key=True) 
    gkg_id = Text(index=True)
    gkg_timestamp = DateTime(partition_key=True)
    gkg_timestamp.truncate_microseconds=True
    source = Text(index=True)
    source_location = Text(index=True)
    parent_input = Text()
    gcam_data = Map(key_type=Text,value_type=Text)
    mft_data = Map(key_type=Text,value_type=Text)
    tone_avg = Float()
    tone_pos = Float()
    tone_neg = Float()
    tone_polarity = Float()
    tone_act_ref_density = Float()
    tone_selfgrp_ref_density = Float()
    wordcount = Integer()
    event_ids = Set(value_type=Text,index=True)
    event_themes = Set(value_type=Text,index=True)
    event_locations = Set(value_type=Text,index=True)
    event_actors = Set(value_type=Text,index=True)
    named_entities = Set(value_type=Text,index=True)
    themes = Set(value_type=Text,index=True)
    
class LocationByGkgId(Model):
    gkg_id = Text(primary_key=True)
    gkg_timestamp = DateTime()
    gkg_timestamp.truncate_microseconds=True
    fullname = Text(primary_key=True,clustering_order='ASC',index=True)
    countrycode = Text(primary_key=True,clustering_order='ASC',index=True)
    adm1code = Text(index=True)
    adm2code = Text(index=True)
    loctype = Integer()
    lat = Float()
    lon = Float()
    feature_id = Text()
    char_offset = Integer()

class LocationByDate(Model):
    gkg_id = Text(primary_key=True)
    gkg_timestamp = DateTime(partition_key=True)
    gkg_timestamp.truncate_microseconds=True
    fullname = Text(primary_key=True,clustering_order='ASC',index=True)
    countrycode = Text(primary_key=True,clustering_order='ASC',index=True)
    adm1code = Text(index=True)
    adm2code = Text(index=True)
    loctype = Integer()
    lat = Float()
    lon = Float()
    feature_id = Text()
    char_offset = Integer()

class ThemeByGkgId(Model):
    gkg_id = Text(primary_key=True)
    gkg_timestamp = DateTime()
    gkg_timestamp.truncate_microseconds=True
    theme = Text(primary_key=True,clustering_order='ASC')
    char_offset = Integer()
    
class ThemeByDate(Model):
    gkg_id = Text(primary_key=True)
    gkg_timestamp = DateTime(partition_key=True)
    gkg_timestamp.truncate_microseconds=True
    theme = Text(primary_key=True,clustering_order='ASC')
    char_offset = Integer()

class EntityByGkgId(Model):
    gkg_id = Text(primary_key=True)
    gkg_timestamp = DateTime()
    gkg_timestamp.truncate_microseconds=True
    entity_name = Text(primary_key=True,clustering_order='ASC')
    entity_type = Text(primary_key=True,clustering_order='ASC')
    char_offset = Integer()

class EntityByDate(Model):
    gkg_id = Text(primary_key=True)
    gkg_timestamp = DateTime(partition_key=True)
    gkg_timestamp.truncate_microseconds=True
    entity_name = Text(primary_key=True,clustering_order='ASC')
    entity_type = Text(primary_key=True,clustering_order='ASC')
    char_offset = Integer()

class EventByDate(Model):
    event_id = Text(primary_key=True)
    gkg_timestamp = DateTime(partition_key=True)
    gkg_timestamp.truncate_microseconds=True
    cameo_data = Map(key_type=Text,value_type=Text)

class EventByDay(Model): #fhopp: new event by day table
    event_id = Text(primary_key=True)
    event_day = DateTime(partition_key=True)
    event_day.truncate_microseconds=True
    event_timestamp = DateTime()
    event_timestamp.truncate_microseconds=True
    cameo_data = Map(key_type=Text,value_type=Text)
    num_mentions = Integer()
    num_sources = Integer()
    num_articles = Integer()
    event_tone_avg = Float()
    event_goldstein = Float()
    event_quadclass = Integer()
    event_code = Text()
    event_base_code = Text()
    event_root_code = Text()
    source_url = Text()
    action_geo_type = Integer()
    action_geo_name = Text()
    action_geo_lat = Float()
    action_geo_long = Float()

class EventByGkgId(Model): 
    gkg_id = Text(primary_key=True)
    event_ids = Set(value_type=Text) 
    events_goldstein = Set(value_type=Text)
    events_quadclass = Set(value_type=Text)
    events_code = Set(value_type=Text)
    
#class GcamReference(Model):
#    variable = Text(primary_key=True)
#    gkg_id = Text(primary_key=True,clustering_order='ASC',index=True)
#    value = Float(index=True)


class StubbornRetryPolicy(RetryPolicy):
    def on_read_timeout(self, *args, **kwargs):
        return self.IGNORE, None
    def on_write_timeout(self, *args, **kwargs):
        return self.IGNORE, None
    def on_unavailable(self, *args, **kwargs):
        return self.IGNORE, None
        

def sync_tables(host=CASSANDRA_CONTACT):
    connection.setup([host], 'gdelt', protocol_version=3)
    sync_table(GkgIdByParent)
    sync_table(GkgIdByUrl)
    sync_table(InputFileByUrl)
    sync_table(GkgRecordByDate)
    sync_table(GkgRecordByDay)
    sync_table(LocationByGkgId)
    sync_table(LocationByDate)
    sync_table(ThemeByGkgId)
    sync_table(ThemeByDate)
    sync_table(EntityByGkgId)
    sync_table(EntityByDate)
    sync_table(EventByDate)
    sync_table(EventByDay) # fhopp: sync w/ the new event table 
    sync_table(EventByGkgId)
    #    sync_table(GcamReference)
    #    sync_table(GcamValue)
    
def establish_session(host=CASSANDRA_CONTACT):
    c = Cluster([host],default_retry_policy=StubbornRetryPolicy())
    s = c.connect('gdelt')
    s.default_timeout = 3000
    return (c, s)

def get_gdelt_url(file_name,url,md5sum,retry_count=0):
    try:
        # TODO: figure out the best way to handle comparison w/ existing DB to minimize execution time and IO load
        raw_data = urllib2.urlopen(url).read()
    except urllib2.HTTPError as e:
        if retry_count < 9:
            print 'HTTP error; retrying {}'.format(retry_count)
            get_gdelt_url(file_name,url,md5sum,retry_count+1)
        else:
            print 'Errors persist after retry number {}; giving up on {}!'.format(retry_count,url)
            #raise e
    except urllib2.URLError as e:
        if retry_count < 9:
            print 'URL error; retrying ({})'.format(retry_count)
            get_gdelt_url(file_name,url,md5sum,retry_count+1)
        else:
            print 'Errors persist after retry number {}; giving up on {}!'.format(retry_count,url)
            #raise e
    else:
        if md5(raw_data).hexdigest() == md5sum:
            file_obj = StringIO(raw_data)
            if mimetypes.guess_type(file_name)[0] == 'application/zip':
                sys.stdout.write('Unzipping file {}...'.format(url))
                sys.stdout.flush()
                file_obj = zipfile.ZipFile(file_obj).open('.'.join(file_name.split('/')[-1].split('.')[:-1]))
            else:
                print 'Found unzipped file {}...'.format(url)
            return file_obj
        else:
            print 'Integrity check failed: checksums did not match! {} {} '.format(url,md5sum)
            if retry_count < 5:
                print 'Redownloading ({}).'.format(retry_count)
                get_gdelt_url(file_name,url,md5sum,retry_count+1)
            else:
                print 'Integrity still cannot be verified on retry {}; giving up on {}!'.format(retry_count,url)
                return None
        
def do_parse(*infile_obj_list):
    # prepare session and all queries before looping
    cluster, session = establish_session()
    update_timestamp_query = session.prepare('UPDATE input_file_by_url SET finish_timestamp=? WHERE file_url=? AND file_timestamp=?;')
    gi_query = session.prepare('SELECT * FROM input_file_by_url WHERE file_url=? AND file_timestamp=?;')
    gi_create_query = session.prepare('INSERT INTO input_file_by_url (file_type,file_name,file_url,file_timestamp,finish_timestamp,included_rows,excluded_rows,start_timestamp) VALUES (?,?,?,?,?,?,?,?);')
    record_query = session.prepare('SELECT gkg_id FROM gkg_id_by_parent WHERE parent_input = ?;')    
    #    gv_create_query = session.prepare('INSERT INTO gcam_value_by_date VALUES ?;')

    if not infile_obj_list:
        print 'No input files specified. Nothing to do. '
        return False
    for infile_type, infile_name, infile_url, infile_md5sum in infile_obj_list:
        sys.stdout.write('Checking database input history for "{}"... '.format(infile_url))
        skipgi = False
        file_timestamp = datetime.strptime(infile_name.split('.')[0],'%Y%m%d%H%M%S')
        gi = session.execute(gi_query,(infile_url,file_timestamp))
        existing_records = []
        if len(gi.current_rows) == 1:
            skipgi = True
            gi = gi[0]
            sys.stdout.write('Found!')
            if infile_type == 'mentions':
                sys.stdout.write('Parsing mentions file, no completeness check!')
                sys.stdout.flush()
            if infile_type == 'event': # fhopp: note that 'event' files have the type 'export' and that there are no checks for event files
                sys.stdout.write('Parsing event file, no completeness check!')
                sys.stdout.flush()
            elif infile_type == 'gkg':
                sys.stdout.write('Checking completeness... ')
                sys.stdout.flush()

                records = [r.gkg_id for r in session.execute(record_query,(infile_url,))]
                if len(records) == gi.included_rows:
                    sys.stdout.write('Expected {} records and found them all!'.format(gi.included_rows))
                    sys.stdout.flush()
                    if not gi.finish_timestamp:
                        print 'Finish timestamp was unset - updating.'
                        session.execute(update_timestamp_query,(datetime.utcnow(),infile_url,file_timestamp))
                    continue
                else:
                    sys.stdout.write('Expected {} records but only found {}. Reprocessing file to create records for unmatched rows. '.format(gi.included_rows,len(records)))
                    sys.stdout.flush()
                    existing_records = records

        elif len(gi.current_rows) == 0:
            sys.stdout.write('Not found, processing new input file. ')
            sys.stdout.flush()

        else:
            raise RuntimeError('Selected input URL "{}" found more than once in the database! That should not happen, so manual intervention is required. Aborting. '.format(infile_url))

        infile_obj = get_gdelt_url(infile_name,infile_url,infile_md5sum)
        if not infile_obj:
            print 'Error loading url {}; skipping. '.format(infile_url)
            continue
        csv_data = []
        csv.field_size_limit(1024**4)
        try:
            gkg_reader = [r for r in csv.reader(infile_obj, delimiter = '\t')]
        except:
            print 'Could not read {} as a proper CSV. Skipping.'.format(infile_url)
            continue
        skip_count = 0
        for i,row in enumerate(gkg_reader):
            if len(row) != EXPECTED_COLUMNS[infile_type]:
                print 'Unexpected row length: {} cells for row {}! Skipping'.format(len(row),i)
                continue
            if infile_type == 'gkg':
                try:
                    sourcetype = int(row[2])
                except ValueError:
                    skip_count += 1
                    continue
                if sourcetype != 1 or 'T' in row[0] or row[3] not in whitelist['source']: #item is not a Web source, was translated, or is not a whitelisted source
                    skip_count += 1
                    continue
            elif infile_type == 'mentions':
                if row[4] not in whitelist['source']:
                    skip_count += 1
                    continue
            elif infile_type =='event': # fhopp: event whitelist check, event table only comes w/ source_url, not 'source' 
                source_url = row[60]
                source_url = source_url[11:].split('/')[0]
                if source_url not in whitelist['source']:
                    skip_count += 1
                    continue
                
            csv_data.append(row)

        infile_obj.close()

        if not skipgi:
            session.execute(gi_create_query,(infile_type,infile_name,infile_url,file_timestamp,None,len(csv_data),skip_count,datetime.utcnow()))
            gi = session.execute(gi_query,(infile_url,file_timestamp))

        print skip_count
        sys.stdout.write('Parsing {} rows. '.format(len(csv_data)))
        sys.stdout.flush()
        if infile_type == 'gkg':
            return gkg_parser(csv_data,existing_records,infile_url,file_timestamp,infile_name,session,cluster)
        elif infile_type == 'event':
            return event_parser(csv_data,infile_url,file_timestamp,infile_name,session,cluster) #fhopp: executes the new event parser 
        elif infile_type == 'mentions':
            return mention_parser(csv_data,infile_url,file_timestamp,infile_name,session,cluster)


def mention_parser(mention_data,infile_url,file_timestamp,infile_name,session,cluster):
    update_timestamp_query = session.prepare('UPDATE input_file_by_url SET finish_timestamp=? WHERE file_url=? AND file_timestamp=?;')
    update_gkg_query = session.prepare('UPDATE gkg_record_by_date SET event_ids=event_ids+? WHERE gkg_timestamp=? AND url=?;')
    update_gkg_day_query = session.prepare('UPDATE gkg_record_by_day SET event_ids=event_ids+? WHERE gkg_day=? AND url=?;')
    #get_gkg_id_query = session.prepare('SELECT gkg_id FROM gkg_id_by_url WHERE url=?;')
    for i,row in enumerate(mention_data):
        sys.stdout.write('.')
        sys.stdout.flush()
        event_id = [row[0]]
        timestamp = parse(row[2])
        gkg_day = timestamp.date()
        gkg_record_url = row[5]
        #gkg_id = session.execute(get_gkg_id_query,(gkg_record_url,))
        session.execute(update_gkg_query,(event_id,timestamp,gkg_record_url))
        session.execute(update_gkg_day_query,(event_id,gkg_day,gkg_record_url))
    session.execute(update_timestamp_query,(datetime.utcnow(),infile_url,file_timestamp))
    sys.stdout.write('Finished {} at {}.'.format(infile_name,datetime.utcnow()))
    sys.stdout.flush()
    session.shutdown()
    cluster.shutdown()
    return True

def event_parser(event_data,infile_url,file_timestamp,infile_name,session,cluster):
    # fhopp: not sure whether these three are the right queries.
    update_timestamp_query = session.prepare('UPDATE input_file_by_url SET finish_timestamp=? WHERE file_url=? AND file_timestamp=?;') # fhopp: not sure this is necessary?
    event_date_create_query = session.prepare('INSERT INTO event_by_date ({}) VALUES ({});'.format(','.join([k for k in EventByDate._columns.keys()]),','.join(['?' for k in EventByDate._columns.keys()])))
    event_day_create_query = session.prepare('INSERT INTO event_by_day ({}) VALUES ({});'.format(','.join([k for k in EventByDay._columns.keys()]),','.join(['?' for k in EventByDay._columns.keys()])))
    #event_gkg_id_create_query = session.prepare('INSERT INTO event_by_gkg_id (event_id,gkg_id) VALUES (?,?);')

    for i,row in enumerate(event_data):
        event_id = row[0]
        sys.stdout.write('.')
        sys.stdout.flush()
        timestamp = parse(row[1])
        event_day = timestamp.date()
        
        cameo_dict = {}
        try:
            cameo_dict['Actor1_Code'] = row[5]
        except:
            cameo_dict['Actor1_Code'] = None
        try:
            cameo_dict['Actor1_CountryCode'] = row[7]
        except:
            cameo_dict['Actor1_CountryCode'] = None
        try:
            cameo_dict['Actor1_KnownGroupCode'] = row[8]
        except:
            cameo_dict['Actor1_KnownGroupCode'] = None

        try:
            cameo_dict['Actor2_Code'] = row[15]
        except:
            cameo_dict['Actor2_Code'] = None
        try:
            cameo_dict['Actor2_CountryCode'] = row[17]
        except:
            cameo_dict['Actor2_CountryCode'] = None
        try:
            cameo_dict['Actor2_KnownGroupCode'] = row[18]
        except:
            cameo_dict['Actor2_KnownGroupCode'] = None

        event_code = row[26]
        event_base_code = row[27]
        event_root_code = row[28]
        event_quadclass = row[29]
        
        try:
            event_goldstein = float(row[30])
        except:
            event_goldstein = -99
            
        num_mentions = row[31]
        num_sources = row[32]
        num_articles = row[33]
        
        try:
            event_tone_avg = float(row[34])
        except:
            event_tone_avg = -99
            
        try:
            action_geo_type = int(row[51])
        except:
            action_geo_type = 'null'
        
        try:
            action_geo_country = str(row[54])
        except:
            action_geo_country = 'null'
        try:
            action_geo_lat = float(row[56])
        except:
            action_geo_lat = -99
        try:
            action_geo_long = float(row[57])
        except:
            action_geo_long = -99
        
        source_url = row[60]

        event_dict = {
            'event_id':event_id,
            'gkg_timestamp':timestamp,
            'cameo_data':cameo_dict,
            'parent_input':infile_url,
            'event_code':event_code,
            'event_base_code':event_base_code,
            'event_root_code':event_root_code,
            'event_quadclass':int(event_quadclass),
            'event_goldstein':float(event_goldstein),
            'num_mentions':int(num_mentions),
            'num_sources':int(num_sources),
            'num_articles':int(num_articles),
            'event_tone_avg':float(event_tone_avg),
            'action_geo_type':action_geo_type,
            'action_geo_name':action_geo_country,
            'action_geo_lat':float(action_geo_lat),
            'action_geo_long':float(action_geo_long),
            'source_url':source_url,
            }

        session.execute(event_date_create_query, event_dict)
        event_dict['event_day'] = event_day
        session.execute(event_day_create_query, event_dict)
        #session.execute(event_gkg_id_create_query,(event_id,gkg_id)
    session.execute(update_timestamp_query,(datetime.utcnow(),infile_url,file_timestamp))
    sys.stdout.write('Finished {} at {}.'.format(infile_name,datetime.utcnow()))
    sys.stdout.flush()
    session.shutdown()
    cluster.shutdown()
    return True
        
def gkg_parser(gkg_data,existing_records,infile_url,file_timestamp,infile_name,session,cluster):
    update_timestamp_query = session.prepare('UPDATE input_file_by_url SET finish_timestamp=? WHERE file_url=? AND file_timestamp=?;')
    theme_create_query = session.prepare('INSERT INTO theme_by_date ({}) VALUES ({});'.format(','.join([k for k in ThemeByDate._columns.keys()]),','.join(['?' for k in ThemeByDate._columns.keys()])))
    loc_create_query = session.prepare('INSERT INTO location_by_date ({}) VALUES ({});'.format(','.join([k for k in LocationByDate._columns.keys()]),','.join(['?' for k in LocationByDate._columns.keys()])))
    gkg_record_create_query = session.prepare('INSERT INTO gkg_record_by_date ({}) VALUES ({});'.format(','.join([k for k in GkgRecordByDate._columns.keys()]),','.join(['?' for k in GkgRecordByDate._columns.keys()])))
    gkg_record_day_create_query = session.prepare('INSERT INTO gkg_record_by_day ({}) VALUES ({});'.format(','.join([k for k in GkgRecordByDay._columns.keys()]),','.join(['?' for k in GkgRecordByDay._columns.keys()])))
    gkg_by_parent_create_query = session.prepare('INSERT INTO gkg_id_by_parent (gkg_id,parent_input) VALUES (?,?);')
    gkg_by_url_create_query = session.prepare('INSERT INTO gkg_id_by_url (gkg_id,url) VALUES (?,?);')

    for i,row in enumerate(gkg_data):
        gkg_id = row[0]
        if gkg_id in existing_records:
            continue            
        sys.stdout.write('.')
        sys.stdout.flush()
        #print 'Row {} of {} ({})'.format(i+1,len(gkg_data),gkg_id)
        timestamp = parse(row[1])
        gkg_day = timestamp.date()
        source = row[3]
        url = row[4]
        gkg_gcam = row[17]
        gcam_dict = {}
        mft_dict = {}
        if len(gkg_gcam) > 0:
            fields = gkg_gcam.split(',')
            fields = filter(None, fields)
            for field in fields:
                f_values = field.split(':')
                gcam_dict[f_values[0]] = f_values[1]
                if f_values[0].startswith('c25'):
                    mft_dict[f_values[0]] = f_values[1]
                #GcamReference.create(gkg_id=gkg_id,variable=f_values[0],value=f_values[1])
#                gvdict = {gkg_id:gkg_id,gkg_timestamp:timestamp}
#                for k,v in gcam_dict.items():
#                        k = ''.join([c if c != '.' else '_' for c in k])
#                        gvdict[k] = v
#                gv = session.execute(gv_create_query,gvdict)

        theme_string = row[8]
        theme_set = []
        for theme in filter(None,theme_string.split(';')):
            theme_vars = theme.split(',')
            if len(theme_vars) < 2:
                continue
            theme_ref = {'gkg_id':gkg_id,'gkg_timestamp':timestamp}
            theme_ref['theme'] = theme_vars[0]
            theme_set.append(theme_vars[0])
            theme_ref['char_offset'] = int(theme_vars[1])
            session.execute(theme_create_query,theme_ref)

        loc_string = row[10]
        loc_set_simple = []
        for loc in filter(None,loc_string.split(';')):
            loc_vars = loc.split('#')
            if len(loc_vars) != 9:
                continue
            loc_ref = {'gkg_id':gkg_id,'gkg_timestamp':timestamp}
            loc_ref['loctype'] = int(loc_vars[0])
            loc_ref['fullname'] = loc_vars[1]
            loc_ref['countrycode'] = loc_vars[2]
            loc_ref['adm1code'] = loc_vars[3]
            loc_ref['adm2code'] = loc_vars[4]
            try:
                loc_ref['lat'] = float(loc_vars[5])
            except:
                loc_ref['lat'] = None
            try:
                loc_ref['lon'] = float(loc_vars[6])
            except:
                loc_ref['lon'] = None
            loc_ref['feature_id'] = loc_vars[7]
            loc_ref['char_offset'] = int(loc_vars[8])
            loc_set_simple.append(loc_ref['adm1code'])
            session.execute(loc_create_query,loc_ref)

        person_string_simple = row[11]
        person_set_simple = person_string_simple.split(';')
        org_string_simple = row[13]
        org_set_simple = org_string_simple.split(';')
        entity_set_simple = org_set_simple+person_set_simple+loc_set_simple

        tone_string = row[15]
        tone_avg,tone_pos,tone_neg,tone_polarity,tone_act_ref_density,tone_selfgrp_ref_density,wordcount = tone_string.split(',')

        gkg_dict = {
            'gkg_id':gkg_id,
            'gkg_timestamp':timestamp,
            'source':source,
            'source_location':whitelist['country_code'][whitelist['source'].index(source)],
            'url':url,
            'gcam_data':gcam_dict,
            'mft_data':mft_dict,
            'parent_input':infile_url,
            'tone_avg':float(tone_avg),
            'tone_pos':float(tone_pos),
            'tone_neg':float(tone_neg),
            'tone_polarity':float(tone_polarity),
            'tone_act_ref_density':float(tone_act_ref_density),
            'tone_selfgrp_ref_density':float(tone_selfgrp_ref_density),
            'wordcount':int(wordcount),
            'event_actors':person_set_simple,
            'event_locations':loc_set_simple,
            'event_organizations':org_set_simple,
            'named_entities': entity_set_simple,
            'themes':theme_set,
        }

        session.execute(gkg_by_parent_create_query,(gkg_id,infile_url))
        session.execute(gkg_record_create_query,gkg_dict)
        gkg_dict['gkg_day'] = gkg_day
        session.execute(gkg_record_day_create_query,gkg_dict)
        session.execute(gkg_by_url_create_query,(gkg_id,url))
    session.execute(update_timestamp_query,(datetime.utcnow(),infile_url,file_timestamp))
#        sys.stdout.write('\n')
    sys.stdout.write('Finished {} at {}.'.format(infile_name,datetime.utcnow()))
    sys.stdout.flush()
    session.shutdown()
    cluster.shutdown()
    return True

def do_parse_pool(infile_obj_list,proc_count=16):
    p = Pool(proc_count)
    p.map_async(do_parse, infile_obj_list).get()
#    do_parse(infile_obj_list[0])
