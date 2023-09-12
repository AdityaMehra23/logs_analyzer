# command for pyspark shell
# pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,com.datastax.spark:spark-cassandra-connector_2.12:3.4.1 

# spark submit command
# ./spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,com.datastax.spark:spark-cassandra-connector_2.12:3.4.1 --py-files ./logs_analyzer/access_log.py ./logs_analyzer/kafka_log_consumer.py

from pyspark.sql import SparkSession
from access_log import parse_log, parse_json_string
import json
from pyspark.sql.functions import col, expr, from_json, udf, to_timestamp, split, date_format
from pyspark.sql.types import StringType, IntegerType, StructType, StructField


# Initialize Spark session
spark = SparkSession.builder \
    .appName("log parsing and storing") \
    .getOrCreate()

# Set the log level to ERROR
spark.sparkContext.setLogLevel("ERROR")

# Kafka broker and topic configuration
kafka_bootstrap_servers = "localhost:9092"
kafka_topic = "web-access-logs"

print("Reading data from kafka")
# Read data from Kafka topic
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .load()

# Deserialize the JSON data in 'value' column
df = df.selectExpr("timestamp","CAST(value AS STRING)")

df = df.withColumnRenamed("timestamp", "process_timestamp")
# show schema
df.printSchema()
print("Parsing JSON string")

# convert json to string
parse_json_udf = udf(parse_json_string, StringType())
# apply udf 
df_string = df.withColumn("value", parse_json_udf(col("value")))

print("Extracting columns from logline")
# parsing logline into columns

parse_log_udf = udf(parse_log, StructType([
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

# apply udf 
df_parsed = df_string.withColumn("parsed_data",parse_log_udf(df_string.value))
# select columns
df_expanded = df_parsed.select("parsed_data.*","process_timestamp")

# modify timestamp column
df_expanded = df_expanded.withColumn("log_timestamp", to_timestamp(col("log_timestamp"), "dd/MMM/yyyy:HH:mm:ss Z"))

# split request column
df_expanded = df_expanded.withColumn("request_type",split(df_expanded["request"]," ").getItem(0)) \
		.withColumn("api",split(df_expanded["request"]," ").getItem(1)) \
		.withColumn("protocol",split(df_expanded["request"]," ").getItem(2))

# drop request column
df_expanded = df_expanded.drop("request")
		
# Extract minute-level timestamp and date for easier analysis
df_expanded = df_expanded.withColumn("process_timestamp", date_format(col("process_timestamp"), "yyyy-MM-dd HH:mm"))
	
df_expanded = df_expanded.withColumn("process_date", date_format(col("process_timestamp"), "yyyy-MM-dd"))	
# show schema
df_expanded.printSchema()

# Function to write data to Cassandra
def write_to_cassandra(df):
    # Configuring Cassandra connection options
    cassandra_options = {
        "keyspace": "sparkanalysis",       # Cassandra keyspace name
        "table": "web_access_time",       # Cassandra table name
        "spark.cassandra.connection.host": "127.0.0.1",  # Cassandra host
        "spark.cassandra.connection.port": "9042"        # Cassandra port
    }
    (df.write.format("org.apache.spark.sql.cassandra").mode("append").options(**cassandra_options).save())

# Write data to Cassandra
out_cassandra = df_expanded.writeStream \
    .foreachBatch(lambda batch_df, batch_id: write_to_cassandra(batch_df)) \
    .outputMode("append") \
    .start()
  
# Write to console for debugging
out_console = df_expanded.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

"""
# Write to HDFS with checkpoints
out_hdfs = df_expanded.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "hdfs://0.0.0.0:9000/log_analyzer/data/web_access_logs") \
    .option("checkpointLocation", "hdfs://0.0.0.0:9000/log_analyzer/checkpoint") \
    .start()
"""    
# Await termination of the query
out_console.awaitTermination()

# Stop the Spark session when done
spark.stop()



