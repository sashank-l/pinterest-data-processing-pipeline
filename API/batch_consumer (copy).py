from kafka import KafkaConsumer
import json
import boto3
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
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from pyspark.sql.functions import col
import shutil

s3_client = boto3.client('s3')
s3 = boto3.resource('s3')

class Batch_Processing:

    def __init__(self) -> None:
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
            .setIfMissing("spark.executor.memory", "6g")
            )
        
        #configure and set credentials
        s3_client = boto3.client('s3')
        session = boto3.Session(profile_name='default')
        credentials = session.get_credentials()
        accessKeyId=credentials.access_key
        secretAccessKey=credentials.secret_key
        cfg.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.0')
        cfg.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
        cfg.set('spark.hadoop.fs.s3a.access.key', accessKeyId)
        cfg.set('spark.hadoop.fs.s3a.secret.key', secretAccessKey)
        self.cfg = cfg

    


    def spark(self):
        
        #set Spark Context
        sc = SparkContext(conf=self.cfg)

        #start Spark session
        spark = SparkSession(sc).builder.appName("TestApp").getOrCreate()

        #read json files from s3 bucket
        df = spark.read.json("s3a://pinterest-data-decf2d83-23f1-4044-9aef-dda97e4934b1/*.json")

        #clean data
        df = df.withColumn('follower_count', f.regexp_replace("follower_count", "User Info Error", "0"))
        df = df.withColumn('follower_count', f.regexp_replace("follower_count", "k", "000"))
        df = df.withColumn('follower_count', f.regexp_replace("follower_count", "M", "000000"))
        df = df.withColumn('follower_count', f.col("follower_count").cast("Int"))
        df = df.withColumn('tag_list', f.regexp_replace("tag_list", "N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e", "None"))

        #narrow down fields necessary
        #df2 = df.select("category","description","follower_count", "tag_list", "title","unique_id").show()

        return df
    
    def create_raw_data_folder(self):
        '''Creates folder for data to be stored
        '''
        if os.path.exists("./raw_data") == False:
            raw_data = os.mkdir("./raw_data")
        print('raw_data directory created')
        
    def Batch_Consumer(self):

        s3_client = boto3.client('s3')
        s3 = boto3.resource('s3')

        batch_consumer = KafkaConsumer("Pinterest_Data_Collection",
            bootstrap_servers='localhost:9092', 
            value_deserializer= lambda x:json.loads(x.decode("utf-8")))

        for msg in batch_consumer:
            print(msg)
            json_file_name = msg.value['unique_id']
            dir = "./raw_data/"
            out_file = open(json_file_name + ".json", "w")
            json.dump(msg, out_file, indent = 6)
            out_file.close()
            s3_client.put_object(
                Body=json.dumps(msg.value),
                Bucket='pinterest-data-decf2d83-23f1-4044-9aef-dda97e4934b1',
                Key=json_file_name+".json"
            )


if __name__ == "__main__":

    
    Batch = Batch_Processing()
    Batch.create_raw_data_folder()
    Batch.Batch_Consumer()