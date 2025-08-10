from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'weather-topic',
    bootstrap_servers='localhost:29092',
    auto_offset_reset='earliest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("Consuming weather data...\n")
try:
    for message in consumer:
        print("Received:", message.value)
except KeyboardInterrupt:
    print("Consumer stopped.")