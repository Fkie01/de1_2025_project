import time
import json
import requests
import sys
import psycopg2

# ----------------------
# Configuration
# ----------------------
TOPIC_NAME = 'sky-telemetry'  # not used here, just reference
OPENSKY_URL = "https://opensky-network.org/api/states/all"

BOUNDING_BOX = {'lamin': 45.83, 'lomin': 5.99, 'lamax': 47.80, 'lomax': 10.49}

# PostgreSQL connection
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

def create_flights_table(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS flights (
            icao24 TEXT,
            callsign TEXT,
            origin_country TEXT,
            time_position BIGINT,
            last_contact BIGINT,
            longitude DOUBLE PRECISION,
            latitude DOUBLE PRECISION,
            baro_altitude DOUBLE PRECISION,
            on_ground BOOLEAN,
            velocity DOUBLE PRECISION,
            true_track DOUBLE PRECISION,
            vertical_rate DOUBLE PRECISION,
            geo_altitude DOUBLE PRECISION,
            ingestion_timestamp DOUBLE PRECISION
        );
    """)
    cur.close()

def insert_flight(cur, record):
    cur.execute("""
        INSERT INTO flights VALUES (
            %(icao24)s, %(callsign)s, %(origin_country)s,
            %(time_position)s, %(last_contact)s,
            %(longitude)s, %(latitude)s, %(baro_altitude)s,
            %(on_ground)s, %(velocity)s, %(true_track)s,
            %(vertical_rate)s, %(geo_altitude)s, %(ingestion_timestamp)s
        )
    """, record)

# ----------------------
# Fetch flight data
# ----------------------
def fetch_flight_data():
    try:
        response = requests.get(OPENSKY_URL, params=BOUNDING_BOX, timeout=10)
        response.raise_for_status()
        data = response.json()
        return data.get('states', [])
    except requests.exceptions.RequestException as e:
        print(f"Network error: {e}")
        return []

# ----------------------
# Main loop
# ----------------------
def main():
    conn = get_pg_connection()
    create_flights_table(conn)
    cur = conn.cursor()

    print("Starting OpenSky ingestion into PostgreSQL...")
    print("Press Ctrl+C to stop.")

    try:
        while True:
            start_time = time.time()
            flights = fetch_flight_data()

            if flights:
                count = 0
                for flight in flights:
                    flight_record = {
                        "icao24": flight[0],
                        "callsign": flight[1].strip() if flight[1] else None,
                        "origin_country": flight[2],
                        "time_position": flight[3],
                        "last_contact": flight[4],
                        "longitude": flight[5],
                        "latitude": flight[6],
                        "baro_altitude": flight[7],
                        "on_ground": flight[8],
                        "velocity": flight[9],
                        "true_track": flight[10],
                        "vertical_rate": flight[11],
                        "geo_altitude": flight[13],
                        "ingestion_timestamp": time.time()
                    }
                    try:
                        insert_flight(cur, flight_record)
                        count += 1
                    except Exception as e:
                        print(f"❌ Failed to insert flight: {e}")

                print(f"Inserted {count} flights into PostgreSQL.")

            # Respect API limits (~10s for anonymous)
            time.sleep(10)

    except KeyboardInterrupt:
        print("Stopping ingestion...")

    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()
