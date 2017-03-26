import re
from datetime import datetime
from pyspark import SparkContext

EXCLUDE_LOGS = ["GET /sign_in/ HTTP/1.1","GET / HTTP/1.1", ""]

def get_top_visited_api(rdd, ascending=False):
	return rdd.map(lambda x:(x[2],1)).reduceByKey(lambda x,y:x+y).map(lambda x:(x[1],x[0])).sortByKey(ascending)

def get_response_code_stats(rdd, ascending=False):
	return rdd.map(lambda x:(x[3],1)).reduceByKey(lambda x,y:x+y).map(lambda x:(x[1],x[0])).sortByKey(ascending)

def get_day_wise_stats(rdd , ascending=False):
	# remove time from date and initialize tuple with key being date and value as 1.
	new_rdd = rdd.map(lambda x:(str(datetime.strptime(x[1],'%d/%b/%Y:%H:%M:%S +0000').date()),1))
	return new_rdd.reduceByKey(lambda x,y:x+y).sortByKey(ascending)


sc = SparkContext()
OUTPUT_PATH = ""
FILE_PATH = "access_log_path"
rdd = sc.textFile(FILE_PATH)
# split log file into lines
lines = rdd.map(lambda x: x.split('\n')[0])
# replace unnecessary character strings
lines = lines.map(lambda x: x.replace("-",""))
# split row into columns
r1 = lines.map( lambda x: map("".join, re.findall(r'\"(.*?)\"|\[(.*?)\]|(\S+)', x)))
# exclude unnecessary logs
r2 = r1.filter(lambda x: x[2] not in EXCLUDE_LOGS)
r3 = get_top_visited_api(r2)
r3.saveAsTextFile(OUTPUT_PATH + 'top_visited_api_stats')
r3 = get_response_code_stats(r2)
r3.saveAsTextFile(OUTPUT_PATH + 'response_code_stats')
r3 = get_day_wise_stats(r2, True)
r3.saveAsTextFile(OUTPUT_PATH + 'day_wise_stats')
