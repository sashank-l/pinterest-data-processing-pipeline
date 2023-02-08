from kafka import KafkaConsumer
import json
import boto3

s3_client = boto3.client('s3')
s3 = boto3.resource('s3')

batch_consumer = KafkaConsumer("Pinterest_Data_Collection",
    bootstrap_servers='localhost:9092', 
    value_deserializer= lambda x:json.loads(x.decode("utf-8")))

for msg in batch_consumer:
    print(msg)
    json_file_name = msg.value['unique_id']
    out_file = open(json_file_name + ".json", "w")
    json.dump(msg, out_file, indent = 6)
    out_file.close()
    s3_client.put_object(
        Body=json.dumps(msg.value),
        Bucket='pinterest-data-decf2d83-23f1-4044-9aef-dda97e4934b1',
        Key=json_file_name+".json"
    )
