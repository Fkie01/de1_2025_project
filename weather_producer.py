import time
import json
import requests
import psycopg2

# ----------------------
# Configuration
# ----------------------
HEADERS = {"User-Agent": "MyWeatherApp/1.0 (myemail@example.com)"}
PG_HOST = "localhost"
PG_PORT = 5432
PG_DB = "aero_traffic"
PG_USER = "admin"
PG_PASSWORD = "password123"

# ----------------------
# PostgreSQL helpers
# ----------------------
def get_pg_connection():
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD
    )
    conn.autocommit = True
    return conn

def create_weather_table(conn, table_name):
    cur = conn.cursor()
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            type TEXT,
            geometry JSONB,
            properties JSONB,
            source TEXT,
            ingested_at DOUBLE PRECISION
        );
    """)
    cur.close()

def insert_weather(cur, table_name, record):
    cur.execute(f"""
        INSERT INTO {table_name} (type, geometry, properties, source, ingested_at)
        VALUES (%(type)s, %(geometry)s, %(properties)s, %(source)s, %(ingested_at)s)
    """, record)

# ----------------------
# Fetchers
# ----------------------
def fetch_isigmet():
    url = "https://aviationweather.gov/api/data/isigmet"
    params = {"loc": "all", "hazard": "all", "format": "json"}
    r = requests.get(url, params=params, headers=HEADERS, timeout=10)
    r.raise_for_status()
    return r.json()  # returns a list

def fetch_gairmet():
    url = "https://aviationweather.gov/api/data/gairmet"
    params = {"type": "all", "format": "json"}
    r = requests.get(url, params=params, headers=HEADERS, timeout=10)
    r.raise_for_status()
    return r.json()  # returns dict with 'features'

# ----------------------
# Normalizers
# ----------------------
def normalize_isigmet(isigmet_list):
    normalized = []
    for item in isigmet_list:
        if not isinstance(item, dict):
            continue
        geometry = None
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
            "geometry": json.dumps(geometry),
            "properties": json.dumps(item),
            "source": "ISIGMET",
            "ingested_at": time.time()
        })
    return normalized

def normalize_gairmet(gairmet_raw):
    normalized = []
    if isinstance(gairmet_raw, dict):
        features = gairmet_raw.get("features", [])
        for item in features:
            normalized.append({
                "type": item.get("properties", {}).get("hazard", "UNKNOWN"),
                "geometry": json.dumps(item.get("geometry")),
                "properties": json.dumps(item.get("properties")),
                "source": "GAIRMET",
                "ingested_at": time.time()
            })
    elif isinstance(gairmet_raw, list):
        for item in gairmet_raw:
            geometry = None
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
                "geometry": json.dumps(geometry),
                "properties": json.dumps(item),
                "source": "GAIRMET",
                "ingested_at": time.time()
            })
    return normalized

# ----------------------
# Main Loop
# ----------------------
def main():
    conn = get_pg_connection()
    cur = conn.cursor()
    create_weather_table(conn, "isigmet")
    create_weather_table(conn, "gairmet")

    print("Weather ingestion into PostgreSQL running...")
    print("Streaming ISIGMET + GAIRMET advisories every 1 minute")

    try:
        while True:
            # ISIGMET
            isig_raw = fetch_isigmet()
            isig_list = normalize_isigmet(isig_raw)
            for record in isig_list:
                insert_weather(cur, "isigmet", record)
            print(f"✔ Inserted {len(isig_list)} ISIGMET advisories")

            # GAIRMET
            g_raw = fetch_gairmet()
            g_list = normalize_gairmet(g_raw)
            for record in g_list:
                insert_weather(cur, "gairmet", record)
            print(f"✔ Inserted {len(g_list)} GAIRMET advisories")

            time.sleep(60)

    except KeyboardInterrupt:
        print("Stopping ingestion...")

    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()
