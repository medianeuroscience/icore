from mg_parse_gkg_cassandra.py import * # do I need to run or just import this? 

# c,s = establish_session() # fhopp: not sure I need to run this? 

f = open('masterfilelist_english.txt').readlines()

lines = []
for line in f:
        lines.append(line.strip('\n').split())

urls = []
for line in lines:
        for text in line:
                if text.startswith('http:'):
                        urls.append(text)

infile_obj_list = []
for url in urls:
        get_gdelt_url(url)
        infile_obj_list.append(file_object)

do_parse(infile_obj_list)


        


