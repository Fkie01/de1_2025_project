import os
import json
import logging
from pyflink.common import SimpleStringSchema, WatermarkStrategy, Types, Configuration 
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.functions import MapFunction, KeyedProcessFunction, CoProcessFunction
from pyflink.datastream.state import ValueStateDescriptor
from shapely.geometry import Point, Polygon

# ============================================================================
# 1. DATA PARSING (Adapted to match your Producer output)
# ============================================================================

class ParseFlightData(MapFunction):
    """Parses JSON from sky-telemetry topic."""
    def map(self, value):
        try:
            # Our producer already sends a clean dict, not a raw list
            return json.loads(value) 
        except Exception as e:
            logging.error(f"Error parsing flight: {e}")
            return None

class ParseWeatherData(MapFunction):
    """Parses JSON from weather-turbulence topic."""
    def map(self, value):
        try:
            return json.loads(value)
        except Exception as e:
            logging.error(f"Error parsing weather: {e}")
            return None

# ============================================================================
# 2. YOUR LOGIC (State & Spatial)
# ============================================================================

class TrackAircraftState(KeyedProcessFunction):
    """Stateful processing: Remembers previous position to calculate updates."""
    def open(self, runtime_context):
        self.aircraft_state = runtime_context.get_state(ValueStateDescriptor(
            "aircraft_state", Types.PICKLED_BYTE_ARRAY()
        ))
    
    def process_element(self, value, ctx):
        if not value: return
        
        prev_state = self.aircraft_state.value() or {}
        current_time = value.get('ingestion_timestamp', 0)
        prev_time = prev_state.get('last_contact', 0)
        
        updated_state = {
            'icao24': value.get('icao24'),
            'lat': value.get('latitude'),
            'lon': value.get('longitude'),
            'alt': value.get('baro_altitude'),
            'velocity': value.get('velocity'),
            'last_contact': current_time,
            'processed_by': 'Flink_State_Machine'
        }
        
        self.aircraft_state.update(updated_state)
        
        # Pass the rich state downstream
        yield updated_state

class SpatialJoinWeather(CoProcessFunction):
    """Joins real-time flights with cached weather hazards."""
    def __init__(self):
        # In a real cluster, this should be a MapState, but dict works for simplified testing
        self.weather_zones = {} 

    def process_element1(self, flight, ctx):
        # Stream 1: Flight Data
        lat = flight.get('lat')
        lon = flight.get('lon')
        
        hazards = []
        if lat and lon:
            for zone in self.weather_zones.values():
                # Simple bounding box check first (optimization)
                # Then expensive polygon check
                if self.point_in_polygon(lat, lon, zone.get('geometry')):
                     hazards.append(zone.get('hazard_type'))
        
        flight['active_hazards'] = hazards
        yield flight

    def process_element2(self, weather, ctx):
        # Stream 2: Weather Data (Updates the lookup table)
        if weather:
            # Create a unique ID for this report
            w_id = f"{weather.get('turbulence_type')}_{weather.get('time_observed')}"
            
            # Construct a small box around the point for demo purposes
            # (Since PIREPs are points, not polygons, we buffer them)
            lat = weather.get('latitude')
            lon = weather.get('longitude')
            
            # Create a pseudo-zone 0.5 degrees around the report
            self.weather_zones[w_id] = {
                'hazard_type': weather.get('turbulence_intensity'),
                'geometry': [
                    (lon-0.5, lat-0.5), (lon+0.5, lat-0.5),
                    (lon+0.5, lat+0.5), (lon-0.5, lat+0.5)
                ]
            }

    def point_in_polygon(self, lat, lon, poly_coords):
        if not poly_coords: return False
        try:
            point = Point(lon, lat)
            polygon = Polygon(poly_coords)
            return polygon.contains(point)
        except:
            return False

# ============================================================================
# 3. THE PIPELINE (Wiring it all together)
# ============================================================================

def main():
    # 1. Setup Environment WITH Configuration
    # This forces the JAR to be loaded before any Java classes are called
    config = Configuration()
    current_dir = os.getcwd()
    jar_path = f"file://{current_dir}/jars/flink-sql-connector-kafka-3.1.0-1.18.jar"    
    print(f"🔗 Loading JAR from: {jar_path}")
    config.set_string("pipeline.jars", jar_path)
    
    # Initialize env with the config
    env = StreamExecutionEnvironment.get_execution_environment(config)
    env.set_parallelism(1)

    # 3. Define Sources (Connecting to Redpanda)
    flight_source = KafkaSource.builder() \
        .set_bootstrap_servers("localhost:19092") \
        .set_topics("sky-telemetry") \
        .set_group_id("flink_processor_group") \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    weather_source = KafkaSource.builder() \
        .set_bootstrap_servers("localhost:19092") \
        .set_topics("weather-turbulence") \
        .set_group_id("flink_weather_group") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    # 4. Create Streams
    raw_flights = env.from_source(flight_source, WatermarkStrategy.no_watermarks(), "Redpanda Flights")
    raw_weather = env.from_source(weather_source, WatermarkStrategy.no_watermarks(), "Redpanda Weather")

    # 5. Process Flights (Parse -> Key -> Stateful Enrich)
    parsed_flights = raw_flights.map(ParseFlightData()).filter(lambda x: x is not None)
    
    # Key by ICAO24 so the same plane always goes to the same state machine
    keyed_flights = parsed_flights \
        .key_by(lambda x: x['icao24']) \
        .process(TrackAircraftState())

    # 6. Process Weather
    parsed_weather = raw_weather.map(ParseWeatherData()).filter(lambda x: x is not None)

    # 7. Connect Streams (Spatial Join)
    # We broadcast weather so every flight processing node knows about all weather
    enriched_stream = keyed_flights \
        .connect(parsed_weather.broadcast()) \
        .process(SpatialJoinWeather())

    # 8. Sink (Print to Console for Verification)
    print("🚀 Flink Job Initialized. Waiting for data...")
    enriched_stream.print()

    # 9. Execute
    env.execute("Aero-Spatial Processor")

if __name__ == '__main__':
    main()