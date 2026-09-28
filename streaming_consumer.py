from pyspark.sql import SparkSession
from pyspark.sql.functions import col,from_json,max,min
from pyspark.sql.types import StructType, StructField, StringType
import os
import pyspark.sql.functions as f

#Maven repository address
os.environ["PYSPARK_SUBMIT_ARGS"] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,org.postgresql:postgresql:42.5.4 API/streaming_consumer.py pyspark-shell'
kafka_topic_name = "Pinterest_Data_Collection"
kafka_bootstrap_servers = 'localhost:9092'

spark = SparkSession \
    .builder \
    .appName("Kafka") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

data_df = spark \
    .readStream \
    .format("Kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic_name) \
    .option("startingOffsets", "earliest")\
    .load()

    
def clean_streamed_data(df, epoch_id):
    #clean data
    df = df.withColumn('follower_count', f.regexp_replace("follower_count", "User Info Error", "None"))
    df = df.withColumn('follower_count', f.regexp_replace("follower_count", "k", "000"))
    df = df.withColumn('follower_count', f.regexp_replace("follower_count", "M", "000000"))
    df = df.withColumn('follower_count', f.col("follower_count").cast("Int"))
    df = df.withColumn('downloaded', f.col("downloaded").cast("Int"))
    df = df.withColumn('index', f.col("index").cast("Int"))
    df = df.withColumn('tag_list', f.regexp_replace("tag_list", "N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e", "None"))
    df = df.withColumn('title', f.when(col('title') =="", None).otherwise(col('title')))
    df = df.withColumn('description', f.when(col('description') =="", None).otherwise(col('description')))
    df = df.withColumn('image_src', f.when(col('image_src') =="", None).otherwise(col('image_src')))
    df = df.withColumn('save_location', f.regexp_replace("save_location", "Local save in ", ""))
    df.write \
        .format("jdbc") \
        .mode("append")   \
        .option("url", "jdbc:postgresql://localhost:5432/pinterest_streaming") \
        .option("dbtable", "experimental_data") \
        .option("user", "postgres") \
        .option("password", "T00narmypos") \
        .option("driver", "org.postgresql.Driver") \
        .save()
    
schema = StructType([
        StructField("category",StringType(),True),
        StructField("index",StringType(), True),
        StructField("unique_id",StringType(), True),
        StructField("title",StringType(), True),
        StructField("description",StringType(), True),
        StructField("follower_count",StringType(), True),
        StructField("tag_list",StringType(), True),
        StructField("is_image_or_video",StringType(), True),
        StructField("image_src",StringType(), True),
        StructField("downloaded",StringType(), True),
        StructField("save_location",StringType(), True),
    ])


data_df = data_df.selectExpr("CAST (value as STRING)")
data_df = data_df.withColumn("value",from_json(col("value"),schema)).select(col("value.*")) 
data_df.writeStream.foreachBatch(clean_streamed_data).start().awaitTermination()
