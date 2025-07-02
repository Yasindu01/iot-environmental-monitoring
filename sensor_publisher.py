import paho.mqtt.client as mqtt
import random
import time
import json

broker = "localhost"
port = 1883

topics = {
    "temperature": "/sensor/temperature",
    "humidity": "/sensor/humidity",
    "air_quality": "/sensor/air_quality"
}

client = mqtt.Client()
client.connect(broker, port, 60)

def publish_sensor_data():
    while True:
        sensor_id = random.choice(["sensor-A", "sensor-B", "sensor-C"])
        temp = round(random.uniform(20.0, 35.0), 2)
        humidity = random.randint(40, 80)
        air_quality = random.choice(["Good", "Moderate", "Unhealthy", "Hazardous"])

        temperature_msg = json.dumps({"value": temp, "unit": "C", "sensor_id": sensor_id})
        humidity_msg = json.dumps({"value": humidity, "unit": "%", "sensor_id": sensor_id})
        air_quality_msg = json.dumps({"status": air_quality, "sensor_id": sensor_id})

        client.publish(topics["temperature"], temperature_msg)
        client.publish(topics["humidity"], humidity_msg)
        client.publish(topics["air_quality"], air_quality_msg)

        print(f"[SENT] ID: {sensor_id} | Temp: {temp}°C | Humidity: {humidity}% | AQI: {air_quality}")
        time.sleep(5)

if __name__ == "__main__":
    publish_sensor_data()
