import time
import json
import requests
import sys
from kafka import KafkaProducer
from kafka.errors import KafkaError

# --- Configuration ---
# Redpanda matches your docker-compose external port
BOOTSTRAP_SERVERS = ['localhost:19092'] 
TOPIC_NAME = 'sky-telemetry'
OPENSKY_URL = "https://opensky-network.org/api/states/all"

# Bounding Box (Lat/Lon) - Currently set to Switzerland/Central Europe to limit data volume
# Format: [min_lat, min_lon, max_lat, max_lon]
# You can comment this out to get GLOBAL data, but it will be heavy.
BOUNDING_BOX = {'lamin': 45.83, 'lomin': 5.99, 'lamax': 47.80, 'lomax': 10.49} 

def create_producer():
    """Creates a reliable Kafka producer."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            # Reliability settings
            retries=5, 
            request_timeout_ms=10000
        )
        print(f" Connected to Redpanda at {BOOTSTRAP_SERVERS}")
        return producer
    except Exception as e:
        print(f" Failed to connect to Redpanda: {e}")
        sys.exit(1)

def fetch_flight_data():
    """Fetches data from OpenSky API with error handling."""
    try:
        # Anonymous users: ~10 second limit. Registered: ~5s.
        response = requests.get(OPENSKY_URL, params=BOUNDING_BOX, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            # 'states' contains the list of flights
            return data.get('states', [])
        else:
            print(f" OpenSky API Error: {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        print(f" Network Error: {e}")
        return None

def main():
    producer = create_producer()
    
    print(" Starting OpenSky Ingestion...")
    print("Press Ctrl+C to stop.")

    while True:
        start_time = time.time()
        
        flights = fetch_flight_data()
        
        if flights:
            count = 0
            # OpenSky returns a list of lists. We convert each flight to a clean JSON object.
            for flight in flights:
                flight_record = {
                    "icao24": flight[0],
                    "callsign": flight[1].strip(),
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
                    # Add a timestamp for when WE ingested it
                    "ingestion_timestamp": time.time()
                }
                
                # Send to Redpanda
                # usage: producer.send(topic, value=data)
                producer.send(TOPIC_NAME, value=flight_record)
                count += 1
            
            # Flush ensures data is actually sent over the network
            producer.flush()
            print(f" Ingested {count} flights.")
        
        # Reliability: OpenSky Anonymous API rate limit is roughly every 10 seconds.
        # We enforce a sleep to ensure we don't get 429 errors.
        time.sleep(10)

if __name__ == "__main__":
    main()