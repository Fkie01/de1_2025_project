import time
import json
import requests
from kafka import KafkaProducer

BOOTSTRAP_SERVERS = ["localhost:19092"]
TOPIC_ISIGMET = "weather-isigmet"
TOPIC_GAIRMET = "weather-gairmet"

HEADERS = {"User-Agent": "MyWeatherApp/1.0 (myemail@example.com)"}


def create_producer():
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=5,
        request_timeout_ms=10000
    )


# ----------------------
# Fetchers
# ----------------------
def fetch_isigmet():
    url = "https://aviationweather.gov/api/data/isigmet"
    params = {"loc": "all", "hazard": "all", "format": "json"}
    r = requests.get(url, params=params, headers=HEADERS, timeout=10)
    r.raise_for_status()
    return r.json()   # <-- RETURNS A LIST


def fetch_gairmet():
    url = "https://aviationweather.gov/api/data/gairmet"
    params = {"type": "all", "format": "json"}
    r = requests.get(url, params=params, headers=HEADERS, timeout=10)
    r.raise_for_status()
    return r.json()   # <-- RETURNS GeoJSON (dict with "features")
    

# ----------------------
# Normalizers
# ----------------------
def normalize_isigmet(isigmet_list):
    """Convert ISIGMET list items into Kafka-friendly records."""
    normalized = []

    for item in isigmet_list:
        if not isinstance(item, dict):
            continue  # skip if item is a list or unexpected

        geometry = None

        # 1. points: "lat,lon lat,lon ..."
        if "points" in item and isinstance(item["points"], str):
            coords = []
            try:
                for pair in item["points"].split():
                    lat, lon = map(float, pair.split(","))
                    coords.append([lat, lon])
                if len(coords) >= 3:
                    geometry = {"type": "Polygon", "coordinates": [coords]}
            except:
                geometry = None

        # 2. latlonpairs: "lat lon lat lon ..."
        elif "latlonpairs" in item and isinstance(item["latlonpairs"], str):
            coords = []
            tokens = item["latlonpairs"].split()
            try:
                it = iter(tokens)
                for lat, lon in zip(it, it):
                    coords.append([float(lat), float(lon)])
                if len(coords) >= 3:
                    geometry = {"type": "Polygon", "coordinates": [coords]}
            except:
                geometry = None

        normalized.append({
            "type": item.get("hazard", "UNKNOWN"),
            "geometry": geometry,
            "properties": item,
            "source": "ISIGMET",
            "ingested_at": time.time()
        })

    return normalized




def normalize_gairmet(gairmet_raw):
    """
    Convert GAIRMET response to a list of Kafka-friendly messages.
    Handles both GeoJSON dict and plain list.
    """
    normalized = []

    if isinstance(gairmet_raw, dict):
        features = gairmet_raw.get("features", [])
        for item in features:
            normalized.append({
                "type": item.get("properties", {}).get("hazard", "UNKNOWN"),
                "geometry": item.get("geometry"),
                "properties": item.get("properties"),
                "source": "GAIRMET",
                "ingested_at": time.time()
            })
    elif isinstance(gairmet_raw, list):
        for item in gairmet_raw:
            if not isinstance(item, dict):
                continue
            geometry = None
            # Handle latlonpairs or points for list-based GAIRMET
            if "points" in item and isinstance(item["points"], str):
                coords = []
                try:
                    for pair in item["points"].split():
                        lat, lon = map(float, pair.split(","))
                        coords.append([lat, lon])
                    if len(coords) >= 3:
                        geometry = {"type": "Polygon", "coordinates": [coords]}
                except:
                    geometry = None
            elif "latlonpairs" in item and isinstance(item["latlonpairs"], str):
                coords = []
                try:
                    tokens = item["latlonpairs"].split()
                    it = iter(tokens)
                    for lat, lon in zip(it, it):
                        coords.append([float(lat), float(lon)])
                    if len(coords) >= 3:
                        geometry = {"type": "Polygon", "coordinates": [coords]}
                except:
                    geometry = None

            normalized.append({
                "type": item.get("hazard", "UNKNOWN"),
                "geometry": geometry,
                "properties": item,
                "source": "GAIRMET",
                "ingested_at": time.time()
            })
    else:
        print("⚠ GAIRMET response is neither dict nor list")

    return normalized



# ----------------------
# Producer Main Loop
# ----------------------
def main():
    producer = create_producer()

    print("Weather producer running...")
    print("Streaming ISIGMET + GAIRMET advisories every 1 minutes")

    while True:
        try:
            # ----------- ISIGMET -----------
            isig_raw = fetch_isigmet()
            isig_list = normalize_isigmet(isig_raw)

            for record in isig_list:
                record["ingested_at"] = time.time()
                producer.send(TOPIC_ISIGMET, value=record)

            print(f"✔ Streamed {len(isig_list)} ISIGMET advisories")


            # ----------- GAIRMET -----------
            g_raw = fetch_gairmet()
            g_list = normalize_gairmet(g_raw)

            for item in g_list:
                record = {
                    "type": item["properties"].get("hazard", "UNKNOWN"),
                    "geometry": item.get("geometry"),
                    "properties": item.get("properties"),
                    "source": "GAIRMET",
                    "ingested_at": time.time()
                }
                producer.send(TOPIC_GAIRMET, value=record)

            print(f"✔ Streamed {len(g_list)} GAIRMET advisories")

            producer.flush()

        except Exception as e:
            print("❌ Error:", e)

        time.sleep(60)  # 5 minutes


if __name__ == "__main__":
    main()
