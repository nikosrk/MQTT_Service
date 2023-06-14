FROM python:3.9

WORKDIR /Users/nrekkas/Documents/MQTT_Service/mqtt_subscriber_service

COPY . .

RUN pip install -r requirements.txt

CMD ["python","-u","subscriber.py"]