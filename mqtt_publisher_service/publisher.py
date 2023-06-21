import paho.mqtt.client as mqtt
import random
import time
import json
from datetime import datetime

# MQTT topic to publish the data
weather_topic = "/weather"

# MQTT client
client = mqtt.Client()

# Connect to the MQTT broker
client.connect("mqtt_broker", 1883, 60)

def generate_weather_data():

    timestamp_microseconds = int(datetime.now().timestamp() * 1e9)

    return {
        "time": timestamp_microseconds,
        "temperature": random.uniform(0, 40),  # in Celsiußs
        "humidity": random.uniform(0, 100),  # in percentage
    }

while True:
    weather_data = generate_weather_data()
    print(weather_data['time'])
    client.publish(weather_topic, json.dumps(weather_data))
    # Wait for some time before publishing the next data
    time.sleep(5)
