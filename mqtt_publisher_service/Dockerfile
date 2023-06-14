FROM python:3.9

WORKDIR /Users/nrekkas/Documents/MQTT_Service/mqtt_publisher_service

COPY . .

RUN pip install -r requirements.txt

CMD ["python","-u","publisher.py"]
