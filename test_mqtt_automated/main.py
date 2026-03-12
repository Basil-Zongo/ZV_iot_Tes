

import json
import ssl
import certifi
from pymongo import MongoClient
from datetime import datetime, UTC
import paho.mqtt.client as mqtt

# -----------------------
# MongoDB
# -----------------------
MONGO_URI = "mongodb+srv://slk:Test12345@zviot.k2l8yag.mongodb.net/?retryWrites=true&w=majority&appName=zviot"

mongo_client = MongoClient(MONGO_URI, tlsCAFile=certifi.where())

db = mongo_client["iot"]
collection = db["sensor_data"]

print("MongoDB Connected")

# -----------------------
# MQTT (HiveMQ)
# -----------------------
BROKER = "4424777756ac4fd281226d60d1b96f29.s1.eu.hivemq.cloud"
PORT = 8883
USERNAME = "Grtswitch"
PASSWORD = "Grtswitch@123"
TOPIC = "sensor/data"

def on_connect(client, userdata, flags, rc):
    print("MQTT Connected:", rc)
    client.subscribe(TOPIC)

def on_message(client, userdata, msg):
    try:
        payload = msg.payload.decode()
        print("Received:", payload)

        data = json.loads(payload)

        data["timestamp"] = datetime.now(UTC)

        collection.insert_one(data)

        print("Saved to MongoDB")

    except Exception as e:
        print("Error:", e)

client = mqtt.Client()
client.username_pw_set(USERNAME, PASSWORD)

client.tls_set(certifi.where(), tls_version=ssl.PROTOCOL_TLS)

client.on_connect = on_connect
client.on_message = on_message

client.connect(BROKER, PORT)

client.loop_forever()