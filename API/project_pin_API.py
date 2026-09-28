from fastapi import FastAPI
from pydantic import BaseModel
import uvicorn
from json import dumps
from kafka import KafkaProducer
import json
from kafka import KafkaClient
from kafka.cluster import ClusterMetadata

app = FastAPI()



class Data(BaseModel):
    category: str
    index: int
    unique_id: str
    title: str
    description: str
    follower_count: str
    tag_list: str
    is_image_or_video: str
    image_src: str
    downloaded: int
    save_location: str

producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer= lambda x:json.dumps(x).encode("utf-8"))

@app.post("/pin/")
def get_db_row(item: Data):
    data = dict(item)
    
    producer.send("Pinterest_Data_Collection", data)
    return item

# Create a connection to retrieve metadata
meta_cluster_conn = ClusterMetadata(
    bootstrap_servers="localhost:9092", # Specific the broker address to connect to
)

# retrieve metadata about the cluster
print(meta_cluster_conn.brokers())


# Create a connection to our KafkaBroker to check if it is running
client_conn = KafkaClient(
    bootstrap_servers="localhost:9092", # Specific the broker address to connect to
    client_id="Broker test" # Create an id from this client for reference
)

# Check that the server is connected and running
print(client_conn.bootstrap_connected())
# Check our Kafka version number
print(client_conn.check_version())

if __name__ == '__main__':
    uvicorn.run("project_pin_API:app", host="localhost", port=8000)

