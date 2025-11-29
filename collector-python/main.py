import requests
import pika
import json
import time

RABBIT_URL = "amqp://admin:admin@rabbitmq:5672/"

def connect_rabbit():
    params = pika.URLParameters(RABBIT_URL)
    return pika.BlockingConnection(params)

def fetch_weather():
    url = "https://api.open-meteo.com/v1/forecast?latitude=-23.55&longitude=-46.63&current_weather=true"
    res = requests.get(url)
    data = res.json()
    
    weather = {
        "temperature": data["current_weather"]["temperature"],
        "windspeed": data["current_weather"]["windspeed"],
        "time": data["current_weather"]["time"]
    }
    return weather

while True:
    print("Coletando clima...")

    weather = fetch_weather()
    message = json.dumps(weather)

    conn = connect_rabbit()
    ch = conn.channel()
    ch.queue_declare(queue="weather")

    ch.basic_publish(exchange="", routing_key="weather", body=message)
    print("Enviado:", message)

    conn.close()
    time.sleep(10)
