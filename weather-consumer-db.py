from kafka import KafkaConsumer
import json
import psycopg2

# PostgreSQL connection
conn = psycopg2.connect(
    dbname="weather_db",
    user="postgres",
    password="Mahlatsi#0310",
    host="localhost",
    port="5432"
)
cursor = conn.cursor()

# Create table if not exists
cursor.execute('''
CREATE TABLE IF NOT EXISTS weather_data (
    station_id TEXT,
    lat FLOAT,
    lon FLOAT,
    timestamp TIMESTAMP,
    temperature FLOAT,
    humidity FLOAT,
    wind_speed FLOAT,
    rainfall FLOAT
)
''')
conn.commit()

# Kafka consumer
consumer = KafkaConsumer(
    'weather-topic',
    bootstrap_servers='localhost:29092',
    auto_offset_reset='earliest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("Consuming and storing weather data...")

try:
    for message in consumer:
        data = message.value
        cursor.execute('''
            INSERT INTO weather_data (
                station_id, lat, lon, timestamp,
                temperature, humidity, wind_speed, rainfall
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ''', (
            data['station_id'],
            data['location']['lat'],
            data['location']['lon'],
            data['timestamp'],
            data['temperature'],
            data['humidity'],
            data['wind_speed'],
            data['rainfall']
        ))
        conn.commit()
        print("Inserted:", data)
except KeyboardInterrupt:
    print("Consumer stopped.")
finally:
    cursor.close()
    conn.close()