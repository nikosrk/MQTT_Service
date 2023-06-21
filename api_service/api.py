import requests
import json
from fastapi import FastAPI
from typing import List
import uvicorn
from pydantic import BaseModel
from datetime import datetime


app = FastAPI()

class WeatherRow(BaseModel):
    topic: str
    temperature: float
    humidity: float
    timestamp: str

@app.get("/weather_data", response_model=List[WeatherRow])
def get_weather_data() -> List[WeatherRow]:
    host = 'http://questdb:9000'
    sql_query = "select * from weather_data"
    query_params = {'query': sql_query}

    try:
        response = requests.post(host + '/exec', params=query_params)
        json_response = json.loads(response.text)
        rows = json_response.get('dataset', [])

        weather_data = []
        for row in rows:

            weather_data.append(
                WeatherRow(
                    topic=row[0],
                    temperature=float(row[1]),
                    humidity=float(row[2]),
                    timestamp=row[3]
                )
            )

        return weather_data
    except requests.exceptions.RequestException as e:
        return {"error": str(e)}
    
if __name__ == "__main__":
    uvicorn.run(app, host="localhost", port=8000)