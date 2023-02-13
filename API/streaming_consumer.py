from pyspark.sql import SparkSession
import os

os.environ["PYSPARK_SUBMIT_ARGS"] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1 API/streaming_consumer.py pyspark-shell'
kafka_topic_name = "Pinterest_Data_Collection"
kafka_bootstrap_servers = 'localhost:9092'

spark = SparkSession \
    .builder \
    .appName("Kafka") \
    .getOrCreate()

data_df = spark \
    .readStream \
    .format("Kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic_name) \
    .option("startingOffsets", "earliest")\
    .load()

data_df.writeStream.outputMode("append").format("console").start().awaitTermination()