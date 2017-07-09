'''
    Spark to pull out some E-commerce insights/reports. 
    Some of them are below:
        a) get total unique sessions
        b) get total view sessions
        c) get total add to cart sessions
        d) get only view sessions, no add to cart
        e) get abandoned users sessions count
        f) get most period for a particular site.
'''
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark import SQLContext
import json
import math
import arrow

spark = SparkSession \
    .builder \
    .appName("cemInsights") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
    
    
def normal_round(n):
    if n - math.floor(n) < 0.5:
        return math.floor(n)
    return math.ceil(n)

def get_unique_sessions(df):
    return df.select("sid").distinct()

def get_view_sessions(df):
    return df.where(df["action_type"]=="view").select("sid").distinct()

def get_carted_sessions(df):
    return df.where(df["action_type"]=="add_to_cart").select("sid").distinct()

def get_place_order_sessions(df):
    return df.where(df["action_type"]=="place_order").select("sid").distinct()

def get_only_view_sessions(df):
    return df.join(df.where((df.action_type=="add_to_cart") |(df.action_type=="remove_from_cart") | (df.action_type=="place_order") | (df.action_type=='register')) \
                   .select("sid") \
                   .distinct(),
                   "sid", "leftanti").select("sid").distinct()

def get_abandoned_sessions(df):
    return df.join(df.where((df.action_type=="add_to_cart") | (df.action_type=='register') | (df.action_type=='remove_from_cart')| (df.action_type=='view')) \
                   .select("sid") \
                   .distinct(),
                   "sid", "leftanti").select("sid").distinct()

def get_average_place_order_qty(df):
    orders = df.where(df["action_type"]=="place_order")
    total_orders = 0
    total_quantity = 0
    for row in orders.rdd.collect():
        total_orders += 1
        if row and "meta" in row and row["meta"] and "orders" in row["meta"]:
            meta = json.loads(row["meta"])
            for order in meta["orders"]:
                total_quantity += int(order["qty"])
    return normal_round(float(total_quantity) / float(total_orders))

def getDatetimeFromEpoch(epoch=None):
    if epoch is not None:
        epoch = epoch/1000
        utc_date = arrow.get(epoch)
        local = utc_date.to('Asia/Kolkata')
        local = local.datetime
        return local
    else:
        return None

def getHourFromDate(action_time):
    return action_time.hour

def getDayFromDate(action_time):
    return action_time.strftime("%A")

def get_most_active_hour(df):
    orders = df.where(df["action_type"]=="view")
    time_range = orders.rdd.map(lambda x:getHourFromDate(getDatetimeFromEpoch(int(x["epoch"]["$numberLong"]))))
    time_range = time_range.map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y).map(lambda x:(x[1],x[0])).sortByKey(False)
    return time_range.take(3)
    
def get_most_buy_interval(df):
    orders = df.where(df["action_type"]=="place_order")
    time_range = orders.rdd.map(lambda x:getHourFromDate(getDatetimeFromEpoch(int(x["epoch"]["$numberLong"]))))
    time_range = time_range.map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y).map(lambda x:(x[1],x[0])).sortByKey(False)
    return time_range.take(3)

def get_most_active_day(df):
    orders = df.where(df["action_type"]=="view")
    time_range = orders.rdd.map(lambda x:getDayFromDate(getDatetimeFromEpoch(int(x["epoch"]["$numberLong"]))))
    time_range = time_range.map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y).map(lambda x:(x[1],x[0])).sortByKey(False)
    return time_range.take(3)

def get_most_active_buy_day(df):
    orders = df.where(df["action_type"]=="place_order")
    time_range = orders.rdd.map(lambda x:getDayFromDate(getDatetimeFromEpoch(int(x["epoch"]["$numberLong"]))))
    time_range = time_range.map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y).map(lambda x:(x[1],x[0])).sortByKey(False)
    return time_range.take(3)



file_path = "/home/hari/Desktop/actions.json"

df  = spark.read.json(file_path)
total_unique_sessions = get_unique_sessions(df) 
print "All sessions:", total_unique_sessions.count()

total_view_sessions = get_view_sessions(df)
print "Sessions with product views:", total_view_sessions.count()

total_unique_carted_sessions = get_carted_sessions(df)
print "Sessions with Add to Cart:", total_unique_carted_sessions.count()

only_viewed_sessions = get_only_view_sessions(df)
print "Sessions with only product views:", only_viewed_sessions.count()

abandoned_product_sessions = get_abandoned_sessions(df)
print "Sessions with only product views and add to cart:", abandoned_product_sessions.count()

place_order_session = get_place_order_sessions(df)
print "Sessions with Place order:", place_order_session.count()

average_place_order_qty = get_average_place_order_qty(df)
print "Average Place Order Quantity:", average_place_order_qty

most_active_hour = get_most_active_hour(df)
print "Most Active Hour (product views):", most_active_hour

most_buy_interval = get_most_buy_interval(df)
print "Most Orders interval period:", most_buy_interval

most_active_day = get_most_active_day(df)
print "Most Active Day of Week (product views):", most_active_day

most_active_buy_day = get_most_active_buy_day(df)
print "Most Active Buy day of week ", most_active_buy_day







