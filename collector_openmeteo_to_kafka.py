import json
import time
import datetime as dt
import requests
from kafka import KafkaProducer

# --- CONFIG ---
KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "meteo_api"

# Exemple: Paris
LAT = 48.8566
LON = 2.3522
CITY = "Paris"
POLL_SECONDS = 300  # toutes les 5 minutes

API_URL = (
    "https://api.open-meteo.com/v1/forecast"
    f"?latitude={LAT}&longitude={LON}"
    "&hourly=temperature_2m,precipitation,wind_speed_10m"
    "&timezone=Europe%2FParis"
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

def fetch_openmeteo():
    r = requests.get(API_URL, timeout=20)
    r.raise_for_status()
    data = r.json()

    # On prend la dernière mesure horaire disponible
    times = data["hourly"]["time"]
    temp = data["hourly"]["temperature_2m"]
    prec = data["hourly"]["precipitation"]
    wind = data["hourly"]["wind_speed_10m"]

    idx = len(times) - 1
    event = {
        "@timestamp": dt.datetime.utcnow().isoformat() + "Z",
        "source": "open-meteo",
        "city": CITY,
        "location": {"lat": LAT, "lon": LON},
        "meteo_hour": times[idx],  # utile aussi pour ES/Kibana
        "temperature_2m": temp[idx],
        "precipitation": prec[idx],
        "wind_speed_10m": wind[idx],
    }
    return event

def main():
    print(f"Producing to Kafka topic={TOPIC} every {POLL_SECONDS}s ...")
    while True:
        try:
            event = fetch_openmeteo()
            producer.send(TOPIC, value=event)
            producer.flush()
            print("Sent:", event)
        except Exception as e:
            print("ERROR:", e)

        time.sleep(POLL_SECONDS)

if __name__ == "__main__":
    main()