# Running Spark application on standalone cluster
# ./spark/bin/spark-submit \
# --py-files ./logs_analyzer/access_log.py \
# ./logs_analyzer/log_analyzer.py   ./logs_analyzer/access_logs/logfiles.log

# This application in Spark reads the logs file directly 

from pyspark.sql import SparkSession
import sys
from access_log import parse_log, parse_json_string
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Create a Spark session
spark = SparkSession.builder.appName("LogsAnalyzer").getOrCreate()

# Set the log level to ERROR
spark.sparkContext.setLogLevel("ERROR")

logFile = sys.argv[1]
# logFile = "access_logs/logfiles.log"

# limit = 1000

df_logs = spark.read.text(logFile).cache()

parse_udf = udf(parse_log, StructType([
    StructField("ip_address", StringType(), False),
    StructField("remote_log_name", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("log_timestamp", StringType(), False),
    StructField("request", StringType(), False),
    StructField("response_code", IntegerType(), False),
    StructField("response_bytes", IntegerType(), False),
    StructField("referrer", StringType(), False),
    StructField("user_agent", StringType(), False),
    StructField("response_time", IntegerType(), False)
]))               
      
# Apply the UDF to the DataFrame and create a new DataFrame with parsed data
df_parsed_log_struct = df_logs.select(parse_udf(df_logs.value).alias("parsed_data"))

# Select individual columns from the struct
df_parsed_log = df_parsed_log_struct.select("parsed_data.*")

df_parsed_log.printSchema()

df_parsed_log.show(5)
# statistics based on content size
content_sizes = df_parsed_log.rdd.map(lambda row: row.response_bytes)
print("Content Size Avg: %i, Min: %i, Max: %i" % (
    content_sizes.reduce(lambda a, b: a + b) / content_sizes.count(),
    content_sizes.min(),
    content_sizes.max()
))

# statistics based on response codes
response_codes = df_parsed_log.rdd.map(lambda row: row.response_code) 

response_codes_agg_count = response_codes.map(lambda row: (row,1)) \
				         .reduceByKey(lambda x,y: x+y) \
					 .take(10)
				
print("Top 10 Count per response codes",response_codes_agg_count)				
print("Response codes Distinct: %i, Total: %i" % ( \
	response_codes.distinct().count(), \
	response_codes.count() \
))

# users statistics- top 5 users by access counts
top_users = df_parsed_log.rdd.map(lambda row: (row.user_agent,1)) \
			 .reduceByKey(lambda x,y: x+y) \
			 .takeOrdered(5,lambda user: -1*user[1])
			 
print(f"Top 5 users by access counts: {top_users}")


# we can use spark sql
df_parsed_log.createOrReplaceTempView("logs")

# we query number of requests made per user
df_reqs_per_user = spark.sql("select user_agent, count(*) as total_requests from logs group by user_agent")

df_reqs_per_user.show()

df_unique_timestamps = spark.sql("select log_timestamp, count(*) as total_requests from logs group by log_timestamp order by 2")

print("Total unique timestamps:\n")
df_unique_timestamps.show()

print("number of request per IP:\n")
df_reqs_per_ip = spark.sql("select ip_address, count(*) as total_requests from logs group by ip_address order by 2")

df_reqs_per_ip.show()
			 
