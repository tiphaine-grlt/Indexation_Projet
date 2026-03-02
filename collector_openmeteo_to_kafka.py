import json
import time
import datetime as dt
import requests
from kafka import KafkaProducer

# --- CONFIG ---
KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "meteo_api"
POLL_SECONDS = 3600  

CITIES = {
    "Paris": {"lat": 48.8566, "lon": 2.3522},
    "Marseille": {"lat": 43.2965, "lon": 5.3698},
    "Lyon": {"lat": 45.7640, "lon": 4.8357},
    "Lille": {"lat": 50.6292, "lon": 3.0573},
    "Bordeaux": {"lat": 44.8378, "lon": -0.5792}
}

# On stocke ici la dernière "meteo_hour" envoyée pour chaque ville
# Exemple : {"Paris": "2023-10-27T10:00", ...}
last_synced_hour = {city: None for city in CITIES}

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

def fetch_weather_updates(city_name, lat, lon):
    """Récupère uniquement les nouvelles données non envoyées"""
    url = (
        f"https://api.open-meteo.com/v1/forecast"
        f"?latitude={lat}&longitude={lon}"
        f"&hourly=temperature_2m,precipitation,wind_speed_10m"
        f"&timezone=Europe%2FParis"
        f"&past_days=1" 
    )
    
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    data = r.json()

    times = data["hourly"]["time"]
    temp = data["hourly"]["temperature_2m"]
    prec = data["hourly"]["precipitation"]
    wind = data["hourly"]["wind_speed_10m"]

    now_str = dt.datetime.now().isoformat()
    last_h = last_synced_hour[city_name]
    
    new_events = []
    
    for i in range(len(times)):
        # CONDITION : L'heure doit être passée/présente ET plus récente que notre dernier envoi
        if times[i] <= now_str:
            if last_h is None or times[i] > last_h:
                event = {
                    "@timestamp": dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z"),
                    "source": "open-meteo",
                    "city": city_name,
                    "location": {"lat": lat, "lon": lon},
                    "meteo_hour": times[i],
                    "temperature_2m": temp[i],
                    "precipitation": prec[i],
                    "wind_speed_10m": wind[i],
                }
                new_events.append(event)
    
    # Si on a trouvé des nouveaux événements, on met à jour le curseur de temps
    if new_events:
        # Le dernier élément de la liste est le plus récent
        last_synced_hour[city_name] = new_events[-1]["meteo_hour"]

    return new_events

def main():
    print(f"Démarrage de la collecte incrémentale...")

    while True:
        total_sent = 0
        try:
            for city, coords in CITIES.items():
                events = fetch_weather_updates(city, coords["lat"], coords["lon"])
                
                for event in events:
                    producer.send(TOPIC, value=event)
                
                if len(events) > 0:
                    print(f"[{city}] {len(events)} nouvelle(s) mesure(s) envoyée(s).")
                total_sent += len(events)

            producer.flush()
            if total_sent > 0:
                print(f"--- SUCCESS: {total_sent} événements au total à {dt.datetime.now().strftime('%H:%M:%S')} ---")

        except Exception as e:
            print(f"!!! ERROR : {e}")

        time.sleep(POLL_SECONDS)

if _name_ == "_main_":
    main()
