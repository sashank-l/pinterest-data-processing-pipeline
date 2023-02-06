import multiprocessing
import pyspark
import json
from pyspark import SparkContext
from pyspark.sql import SparkSession
import boto3
import os

cfg = (
    pyspark.SparkConf()
    # Setting the master to run locally and with the maximum amount of cpu coresfor multiprocessing.
    .setMaster(f"local[{multiprocessing.cpu_count()}]")
    # Setting application name
    .setAppName("TestApp")
    # Setting config value via string
    .set("spark.eventLog.enabled", False)
    # Setting environment variables for executors to use
    .setExecutorEnv(pairs=[("VAR3", "value3"), ("VAR4", "value4")])
    # Setting memory if this setting was not set previously
    .setIfMissing("spark.executor.memory", "1g")
)

s3_client = boto3.client('s3')
s3 = boto3.resource('s3')
accessKeyId=os.environ["AWS_ACCESS_KEY_ID"]
secretAccessKey=os.environ["AWS_SECRET_ACCESS_KEY"]
cfg.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.0')
cfg.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
cfg.set('spark.hadoop.fs.s3a.access.key', accessKeyId)
cfg.set('spark.hadoop.fs.s3a.secret.key', secretAccessKey)

def spark():
    sc = SparkContext(conf=cfg)
    spark = SparkSession(sc).builder.appName("TestApp").getOrCreate()
    df = spark.read.json("s3a://pinterest-data-decf2d83-23f1-4044-9aef-dda97e4934b1/*.json")
    df.select("category","save_location","title","unique_id").show()
    df.printSchema()
    #df.show()
    #for i in range(0,10):
        #s3_client.download_file('pinterest-data-decf2d83-23f1-4044-9aef-dda97e4934b1', )


#df = spark.read.csv

# Getting a single variable
print(cfg.get("spark.executor.memory"))
# Listing all of them in string readable format
print(cfg.toDebugString())

spark()