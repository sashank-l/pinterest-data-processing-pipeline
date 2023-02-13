import multiprocessing
import pyspark
import json
from pyspark import SparkContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
import boto3
import os
from airflow.models import DAG
from datetime import datetime
from datetime import timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable

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

default_args = {
    'owner': 'balany1',
    'depends_on_past': False,
    'email': ['andrewmcnamara@live.co.uk'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'start_date': datetime(2023, 2, 8), # If you set a datetime previous to the curernt date, it will try to backfill
    'retry_delay': timedelta(minutes=5),
    'end_date': datetime(2024, 1, 1),
}


def spark():
    #configure and set credentials
    s3_client = boto3.client('s3')
    session = boto3.Session(profile_name='default')
    credentials = session.get_credentials()
    #s3 = boto3.resource('s3')#
    #my_bucket = s3_client.bucket('pinterest-data-decf2d83-23f1-4044-9aef-dda97e4934b1')
    accessKeyId=credentials.access_key
    secretAccessKey=credentials.secret_key
    cfg.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.0')
    cfg.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
    cfg.set('spark.hadoop.fs.s3a.access.key', accessKeyId)
    cfg.set('spark.hadoop.fs.s3a.secret.key', secretAccessKey)

    #set Spark Context
    sc = SparkContext(conf=cfg)

    #start Spark session
    spark = SparkSession(sc).builder.appName("TestApp").getOrCreate()

    #read json files from s3 bucket
    df = spark.read.json("s3a://pinterest-data-decf2d83-23f1-4044-9aef-dda97e4934b1/*.json")
    print(df)

    #clean data
    df = df.withColumn('follower_count', f.regexp_replace("follower_count", "User Info Error", "0"))
    df = df.withColumn('follower_count', f.regexp_replace("follower_count", "k", "000"))
    df = df.withColumn('follower_count', f.regexp_replace("follower_count", "M", "000000"))
    df = df.withColumn('follower_count', f.col("follower_count").cast("Int"))
    df = df.withColumn('tag_list', f.regexp_replace("tag_list", "N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e", "None"))

    #narrow down fields necessary
    df = df.select("category","description","follower_count", "tag_list", "title","unique_id").show()

    #for i in range(0,10):
        #s3_client.download_file('pinterest-data-decf2d83-23f1-4044-9aef-dda97e4934b1', )



# Getting a single variable
print(cfg.get("spark.executor.memory"))
# Listing all of them in string readable format
print(cfg.toDebugString())

with DAG(dag_id='spark',
         default_args=default_args,
         schedule_interval='*/1 * * * *',
         catchup=False,
         tags=['test']
         ) as dag:
        spark()