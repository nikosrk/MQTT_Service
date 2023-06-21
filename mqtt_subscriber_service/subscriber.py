import paho.mqtt.client as mqtt
from questdb.ingress import Sender, IngressError, TimestampNanos
import sys
from datetime import datetime
import json

# MQTT topic to subscribe
topic = "/weather"

# QuestDB connection details
questdb_host = "questdb"
questdb_port = 9009
questdb_table = "weather_data"

# MQTT client
client = mqtt.Client()

# Callback function when the client receives a CONNACK response from the MQTT broker
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT broker")
        client.subscribe(topic)
    else:
        print("Connection to MQTT broker failed")

# Callback function when a message is received from the MQTT broker
def on_message(client, userdata, msg):
    payload = json.loads(msg.payload)
    topic = msg.topic
    print("Received message:", payload)

    # Store the data in QuestDB
    store_data_in_questdb(topic, payload)

# Function to store the data in QuestDB
def store_data_in_questdb(topic, payload):
    try:
        with Sender(questdb_host, questdb_port) as sender:
            sender.row(
                questdb_table,
                symbols={
                    'id': topic},
                columns={
                    'temperature': payload['temperature'],
                    'humidity': payload['humidity'],
                    'timestamp': payload['time']},
                at=TimestampNanos.now())
            print('Add Data to DB')
            sender.flush()
    except IngressError as e:
        sys.stderr.write(f'Got error: {e}\n')


# Set the callback functions
client.on_connect = on_connect
client.on_message = on_message

# Connect to the MQTT broker
client.connect("mqtt_broker", 1883, 60)

# Start the MQTT loop to receive messages
client.loop_forever()

