# Base image
FROM python:3.9

WORKDIR /Users/nrekkas/Documents/MQTT_Service/api_service

COPY . .

RUN pip install -r requirements.txt

CMD ["python3", "-m", "uvicorn", "api:app", "--reload", "--host", "0.0.0.0", "--port", "8000"]]
