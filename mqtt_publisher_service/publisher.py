import paho.mqtt.client as mqtt
import random
import time
import json
from datetime import datetime

# MQTT broker details
broker_address = "mqtt.example.com"
broker_port = 1883

# MQTT topic to publish the data
weather_topic = "weather"

# MQTT client
client = mqtt.Client()

# Connect to the MQTT broker
client.connect(broker_address, broker_port)


def generate_weather_data():
    return {
        "time": datetime.now().isoformat(),
        "temperature": random.uniform(0, 40),  # in Celsius
        "humidity": random.uniform(0, 100),  # in percentage
    }


while True:

    weather_data = generate_weather_data()
    client.publish(weather_topic, json.dumps(weather_data))
    # Wait for some time before publishing the next data
    time.sleep(5)

