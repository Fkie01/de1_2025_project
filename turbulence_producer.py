import time
import json
import requests
import sys
from kafka import KafkaProducer
from kafka.errors import KafkaError

# --- Configuration ---
BOOTSTRAP_SERVERS = ['localhost:19092']
TOPIC_NAME = 'weather-turbulence'

# Aviation Weather Center (AWC) Data API - Pilot Reports (PIREPs)
# Format: JSON
AWC_API_URL = "https://aviationweather.gov/api/data/pirep"

# Shared Bounding Box (Switzerland/Central Europe)
# AWC Format: minlon,minlat,maxlon,maxlat
# Matches OpenSky: lon 5.99-10.49, lat 45.83-47.80
BBOX = "5.99,45.83,10.49,47.80"

def create_producer():
    """Creates a reliable Kafka producer."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=5,
            request_timeout_ms=10000
        )
        print(f" Connected to Redpanda at {BOOTSTRAP_SERVERS}")
        return producer
    except Exception as e:
        print(f" Failed to connect to Redpanda: {e}")
        sys.exit(1)

def fetch_turbulence_reports():
    """Fetches PIREPs (Pilot Reports) from NOAA/AWC."""
    params = {
        'format': 'json',
        'bbox': BBOX,
        'age': 1  # Get reports from the last 1 hour
    }
    
    try:
        response = requests.get(AWC_API_URL, params=params, timeout=10)
        
        if response.status_code == 200:
            # The API returns a list of features (GeoJSON style) or raw items
            # We need to verify if the list is empty
            return response.json()
        else:
            print(f" AWC API Error: {response.status_code}")
            return []
            
    except requests.exceptions.RequestException as e:
        print(f" Network Error: {e}")
        return []

def main():
    producer = create_producer()
    
    print(" Starting Turbulence (PIREP) Ingestion...")
    print(f" Polling area: {BBOX}")
    print("Press Ctrl+C to stop.")

    while True:
        reports = fetch_turbulence_reports()
        
        count = 0
        if reports:
            for report in reports:
                # We only care if 'turb' (turbulence) data exists in the report
                # The AWC JSON structure usually puts data in properties
                # But sometimes it's flat. This handles the standard JSON response.
                
                # Check raw format or GeoJSON format
                raw_data = report.get('properties', report) # Fallback if not GeoJSON
                
                # Filter: Only send if there is explicit turbulence info
                # 'turb-int' is often the intensity (Light, Moderate, Severe)
                if 'turb' in raw_data or 'turb-int' in raw_data:
                    
                    cleaned_report = {
                        "raw_text": raw_data.get('raw_text'),
                        "aircraft_type": raw_data.get('ac_type'),
                        "time_observed": raw_data.get('obs_time'),
                        "latitude": report.get('geometry', {}).get('coordinates', [0,0])[1],
                        "longitude": report.get('geometry', {}).get('coordinates', [0,0])[0],
                        "altitude_ft_msl": raw_data.get('alt_ft_msl'),
                        "turbulence_intensity": raw_data.get('turb_int', 'Unknown'),
                        "turbulence_type": raw_data.get('turb_type', 'Unknown'),
                        "ingestion_timestamp": time.time()
                    }
                    
                    producer.send(TOPIC_NAME, value=cleaned_report)
                    count += 1
            
            producer.flush()
            if count > 0:
                print(f"  Ingested {count} turbulence reports.")
            else:
                print("  No active turbulence reports in this region.")
        else:
            print(" No data received.")

        # API Etiquette: PIREPs don't update second-by-second.
        # Polling every 2-5 minutes is standard.
        time.sleep(120) 

if __name__ == "__main__":
    main()