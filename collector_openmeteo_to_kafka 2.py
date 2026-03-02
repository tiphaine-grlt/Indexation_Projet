import json
import time
import datetime as dt
import requests
from kafka import KafkaProducer

# --- CONFIG ---
KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "meteo_api"

LAT = 48.8566
LON = 2.3522
CITY = "Paris"

POLL_SECONDS = 3600  # toutes les 5 minutes

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

def fetch_openmeteo_events(last_n=24):
    r = requests.get(API_URL, timeout=20)
    r.raise_for_status()
    data = r.json()

    times = data["hourly"]["time"]
    temp = data["hourly"]["temperature_2m"]
    prec = data["hourly"]["precipitation"]
    wind = data["hourly"]["wind_speed_10m"]

    # On prend les 24 dernières heures
    start = max(0, len(times) - last_n)

    events = []
    for i in range(start, len(times)):
        event = {
            "@timestamp": dt.datetime.utcnow().isoformat() + "Z",
            "source": "open-meteo",
            "city": CITY,
            "location": {"lat": LAT, "lon": LON},
            "meteo_hour": times[i],
            "temperature_2m": temp[i],
            "precipitation": prec[i],
            "wind_speed_10m": wind[i],
        }
        events.append(event)

    return events

def main():
    print(f"Producing {TOPIC} every {POLL_SECONDS}s (last 24h each time)...")

    while True:
        try:
            events = fetch_openmeteo_events(last_n=24)

            for event in events:
                producer.send(TOPIC, value=event)

            producer.flush()
            print(f"Sent {len(events)} events")

        except Exception as e:
            print("ERROR:", e)

        time.sleep(POLL_SECONDS)

if __name__ == "__main__":
    main()  