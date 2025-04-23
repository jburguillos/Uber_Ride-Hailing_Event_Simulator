import random
import uuid
import json
from datetime import datetime, timedelta
from fastavro import writer, parse_schema
import time
from confluent_kafka import Producer
import io
from fastavro import schemaless_writer

# CONFIGURACIÓN AZURE EVENT HUB
conf = {
    'bootstrap.servers': 'iesstsabbadbaa-grp-01-05.servicebus.windows.net:9093',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': '$ConnectionString',
    'sasl.password': 'Endpoint=sb://iesstsabbadbaa-grp-01-05.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=nzlvep4hLvKw6ssZ4eTU5/8948hr36vJP+AEhHXDArI='  # reemplaza con el real
}

producer = Producer(conf)

def send_avro(topic: str, record: dict, parsed_schema):
    """
    Serializes `record` to Avro binary (no container file) and sends it.
    """
    buf = io.BytesIO()
    schemaless_writer(buf, parsed_schema, record)
    # buf.getvalue() is now the Avro‑encoded bytes
    producer.produce(topic, buf.getvalue())
    producer.flush()

def send_event(topic, data):
    producer.produce(topic, json.dumps(data).encode('utf-8'))
    producer.flush()

# Load configuration
with open("config.json") as f:
    config = json.load(f)

NUM_REQUESTS = config["num_requests"]
ACTIVE_DRIVERS = config["active_drivers"]
PRICING = config["pricing_model"]
EVENT_INTERVAL_SEC = config["event_interval_sec"]
SIMULATION_DAYS = config["simulation_days"]

# Schema definitions
ride_status_schema = {
    "doc": "A ride status recording.",
    "name": "RideStatus",
    "namespace": "acme.status",
    "type": "record",
    "fields": [
        {"name": "ride_id", "type": "string"},
        {"name": "passenger_id", "type": "string"},
        {"name": "driver_id", "type": "string"},
        {"name": "ride_status", "type": {
            "type": "enum",
            "name": "RideStatusEnum",
            "symbols": ["completed", "cancelled"]
        }},
        {"name": "request_time", "type": "long"},
        {"name": "pickup_time", "type": "long"},
        {"name": "dropoff_time", "type": "long"},
        {"name": "ride_duration", "type": "float"},
        {"name": "pickup_location", "type": "string"},
        {"name": "dropoff_location", "type": "string"},
        {"name": "distance", "type": "float"},
        {"name": "price", "type": "float"},
        {"name": "tip", "type": "float"},
        {"name": "vehicle_type", "type": "string"},
        {"name": "cancellation_reason", "type": ["null", "string"], "default": None}
    ]
}

passenger_request_schema = {
    "doc": "A passenger request recording.",
    "name": "PassengerRequest",
    "namespace": "acme.requests",
    "type": "record",
    "fields": [
        {"name": "request_id", "type": "string"},
        {"name": "timestamp", "type": "long"},
        {"name": "passenger_id", "type": "string"},
        {"name": "pickup_location", "type": "string"},
        {"name": "dropoff_location", "type": "string"},
        {"name": "distance", "type": "float"},
        {"name": "status", "type": {
            "type": "enum",
            "name": "RequestStatusEnum",
            "symbols": ["completed", "cancelled"]
        }},
        {"name": "payment_type", "type": ["null", "string"], "default": None}
    ]
}

parsed_ride_schema = parse_schema(ride_status_schema)
parsed_request_schema = parse_schema(passenger_request_schema)

# Constants and behavior
simulation_start = datetime(year=2025, month=4, day=1)
zones = ["Downtown", "Suburbs", "Airport", "University", "Nightlife"]
vehicle_types = ["standard", "premium", "shared"]
zone_cancellation_chance = {"Nightlife": 0.3, "Suburbs": 0.05}

def get_demand_multiplier(hour, zone,vehicle_type):
    base = 0.5
    if 0 <= hour < 4:
        if zone == 'Nightlife':
            base = 0.95
        else:
            base = 0.15
    elif 7 <= hour < 9 or 17 <= hour < 20:
        base = 0.9 if zone in ['Downtown', 'Suburbs'] else 0.6
    elif 12 <= hour < 14:
        base = 0.7
    elif 20 <= hour < 24 and zone == 'Nightlife':
        base = 0.8
    
    noise = random.uniform(-0.05, 0.05)
    
    if vehicle_type == "premium":
        base = base/2
    elif vehicle_type == "shared":
        base = base*3/4
    
    return max(0.05, min(1.0, base + noise))

def get_duration_multiplier(hour, zone):
    if zone == 'Nightlife' and (23 <= hour or hour < 4):
        return random.uniform(1.6, 2.3)
    if zone in ['Downtown', 'Suburbs'] and (7 <= hour < 9 or 17 <= hour < 20):
        return random.uniform(1.5, 2.0)
    return random.uniform(0.8, 1.3)

def generate_ride_event(sim_time):
    hour = sim_time.hour
    pickup_location = random.choice(zones)
    vehicle_type = random.choice(vehicle_types)
    if random.random() > get_demand_multiplier(hour, pickup_location,vehicle_type):
        return None, None

    ride_id = str(uuid.uuid4())
    passenger_id = str(uuid.uuid4())
    driver_id = str(uuid.uuid4()) if random.randint(1, 100) <= ACTIVE_DRIVERS else "unassigned"
    
    dropoff_location = random.choice(zones)
    request_time = int(sim_time.timestamp())
    pickup_delay = random.randint(30, 300)
    pickup_time = request_time + pickup_delay
    base_duration = random.randint(300, 900)
    ride_duration = int(base_duration * get_duration_multiplier(hour, pickup_location))
    dropoff_time = pickup_time + ride_duration
    distance = round(random.uniform(1.0, 20.0), 2)
    if random.randint(1, 1000) == 1:
        distance += 50
    surge = PRICING["surge_zones"].get(pickup_location, 1.0)
    price = round(PRICING["base_price"] + distance * PRICING["price_per_km"] * surge, 2)
    if random.randint(1, 1000) == 1:
        price = price*10
    tip = round(price * PRICING["tip_rate"] if random.random() < 0.5 else 0, 2)

    if random.random() < zone_cancellation_chance.get(pickup_location, 0.1):
        ride_status = "cancelled"
        cancellation_reason = random.choice(["Driver_cancelled", "Passenger_cancelled", "Other"])
        ride_duration = 0
        dropoff_time = 0
        price = 0
        tip = 0
    else:
        ride_status = "completed"
        cancellation_reason = None

    ride_record = {
        "ride_id": ride_id,
        "passenger_id": passenger_id,
        "driver_id": driver_id,
        "ride_status": ride_status,
        "request_time": request_time,
        "pickup_time": pickup_time,
        "dropoff_time": dropoff_time,
        "ride_duration": ride_duration,
        "pickup_location": pickup_location,
        "dropoff_location": dropoff_location,
        "distance": distance,
        "price": price,
        "tip": tip,
        "vehicle_type": vehicle_type,
        "cancellation_reason": cancellation_reason
    }

    request_record = {
        "request_id": str(uuid.uuid4()),
        "timestamp": request_time,
        "passenger_id": passenger_id,
        "pickup_location": pickup_location,
        "dropoff_location": dropoff_location,
        "distance": distance,
        "status": ride_status,
        "payment_type": random.choice(["cash", "card"]) if ride_status == "completed" else None
    }

    return ride_record, request_record

# MAIN
def generate_and_save_data():
    counting = 0
    total_minutes = SIMULATION_DAYS*24*60
    events_per_minute = int(NUM_REQUESTS/(total_minutes))
    for minutes_offset in range(total_minutes):
        sim_time = simulation_start + timedelta(minutes=minutes_offset)
        for i in range(events_per_minute):
            ride, request = generate_ride_event(sim_time)
            if ride and request:
                print(f"🟢 Event {i+1} of minute {minutes_offset+1}: Sending ride_status and passenger_request...")
                #send_event("group1_ride_status", ride)
                #send_event("group1_passenger_requests", request)
                send_avro("group1_ride_status",       ride,    parsed_ride_schema)
                send_avro("group1_passenger_requests", request, parsed_request_schema)
                counting+=1
            else:
                print(f"❌ Event {i+1} of minute {minutes_offset+1}: Not generated")

            time.sleep(EVENT_INTERVAL_SEC)
    print(f"✅ Generated {counting} ride events and passenger requests out of {NUM_REQUESTS}.")

if __name__ == "__main__":
    generate_and_save_data()
