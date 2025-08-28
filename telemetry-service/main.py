import sys


import paho.mqtt.client as mqtt
import time
import json
import random
import os
sys.stdout.reconfigure(line_buffering=True)

# Configuration from environment variables
MQTT_BROKER_HOST = os.getenv("MQTT_BROKER_HOST", "localhost")
MQTT_BROKER_PORT = int(os.getenv("MQTT_BROKER_PORT", 1883))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "vehicle/telemetry")
PUBLISH_INTERVAL_SECONDS = int(os.getenv("PUBLISH_INTERVAL_SECONDS", 5))
CLIENT_ID = f"telemetry-publisher-{random.randint(0, 1000)}"

def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print(f"Connected to MQTT Broker: {MQTT_BROKER_HOST}:{MQTT_BROKER_PORT}")
    else:
        print(f"Failed to connect, return code {rc}\n")

def on_publish(client, userdata, mid, reason_code, properties=None):
    print(f"Published message ID: {mid}, reason: {reason_code}")

def generate_telemetry_data():
    """Generates simulated vehicle telemetry data."""
    data = {
        "timestamp": int(time.time()),
        "vehicle_id": "DEMO_1", # Static for simplicity, could be dynamic
        "engine_temp_c": round(random.uniform(85.0, 105.0), 2),
        "speed_kmh": random.randint(0, 120),
        "fuel_level_percent": random.randint(10, 100),
        "tire_pressure_psi": {
            "front_left": round(random.uniform(32.0, 36.0), 1),
            "front_right": round(random.uniform(32.0, 36.0), 1),
            "rear_left": round(random.uniform(32.0, 36.0), 1),
            "rear_right": round(random.uniform(32.0, 36.0), 1),
        },
        "location": {
            "latitude": round(random.uniform(12.9, 13.0), 6), # Bengaluru coords
            "longitude": round(random.uniform(77.5, 77.6), 6)
        }
    }
    return json.dumps(data)

def run():
    print("Starting MQTT client...")
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=CLIENT_ID)
    client.on_connect = on_connect
    client.on_publish = on_publish
    max_retries = 10
    for attempt in range(1, max_retries + 1):
        try:
            print(f"Trying to connect, attempt {attempt}...")
            client.connect(MQTT_BROKER_HOST, MQTT_BROKER_PORT, 60*2)
            print(f"✅ Connected to MQTT broker on attempt {attempt}")
            break
        except Exception as e:
            print(f"Could not connect to MQTT broker at {MQTT_BROKER_HOST}:{MQTT_BROKER_PORT}. Error: {e}")
            time.sleep(20)
            #exit(1)
    else:
        print("❌ Could not connect after multiple attempts. Exiting.")
        exit(1)
    client.loop_start() # Start background thread for network traffic

    print(f"Publishing telemetry data to topic: '{MQTT_TOPIC}' every {PUBLISH_INTERVAL_SECONDS} seconds...")
    try:
        while True:
            telemetry_data = generate_telemetry_data()
            client.publish(MQTT_TOPIC, telemetry_data, qos=1) # QoS 1 for at least once delivery
            time.sleep(PUBLISH_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        print("Telemetry service stopped.")
    finally:
        client.loop_stop()
        client.disconnect()

if __name__ == "__main__":
    run()
