from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime

# List of weather station locations
stations = [
    {"id": "WS001", "lat": -26.2041, "lon": 28.0473},  # Johannesburg
    {"id": "WS002", "lat": -25.7479, "lon": 28.2293},  # Pretoria
    {"id": "WS003", "lat": -29.8587, "lon": 31.0218},  # Durban
]

producer = KafkaProducer(
    bootstrap_servers='localhost:29092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_weather_data(station):
    return {
        "station_id": station["id"],
        "location": {"lat": station["lat"], "lon": station["lon"]},
        "timestamp": datetime.utcnow().isoformat(),
        "temperature": round(random.uniform(10, 35), 2),
        "humidity": round(random.uniform(20, 90), 1),
        "wind_speed": round(random.uniform(5, 60), 2),
        "rainfall": round(random.uniform(0, 15), 2)
    }

if __name__ == "__main__":
    print("Streaming weather data...")
    try:
        while True:
            for station in stations:
                data = generate_weather_data(station)
                producer.send("weather-topic", value=data)
                print("Sent:", data)
                time.sleep(1)
    except KeyboardInterrupt:
        print("Producer stopped.")
    finally:
        producer.close()
