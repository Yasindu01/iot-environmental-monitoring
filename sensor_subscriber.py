import paho.mqtt.client as mqtt
import json
from datetime import datetime
import csv



broker = "localhost"
port = 1883


topics = [("/sensor/temperature", 0),
          ("/sensor/humidity", 0),
          ("/sensor/air_quality", 0)]


def on_connect(client, userdata, flags, rc):
    print("Connected to MQTT Broker with result code " + str(rc))
    client.subscribe(topics)


def on_message(client, userdata, msg):
    topic = msg.topic
    payload = msg.payload.decode()
    print(f"[RECEIVED] Topic: {topic} | Message: {payload}")

 
    try:
        data = json.loads(payload)
        if topic == "/sensor/temperature":
            store_in_sql(data)
        elif topic == "/sensor/humidity":
            store_in_mongo(data)
        elif topic == "/sensor/air_quality":
            store_in_neo4j(data)
    except Exception as e:
        print("Error:", e)

import mysql.connector

def store_in_sql(data):
    try:
        conn = mysql.connector.connect(
            host="localhost",
            port=3310,
            user="root",
            password="",
            database="sensor_data"
        )
        cursor = conn.cursor()
        sql = "INSERT INTO temperature_readings (value, unit, sensor_id, recorded_at) VALUES (%s, %s, %s, %s)"
        values = (data["value"], data["unit"], data["sensor_id"], datetime.now())
        cursor.execute(sql, values)
        conn.commit()
        print("✅ Stored temperature in SQL:", values)
        export_to_csv(data)
    except Exception as e:
        print("❌ SQL Error:", e)
    finally:
        try:
            cursor.close()
            conn.close()
        except:
            pass

def export_to_csv(data):
    with open("temperature_log.csv", mode="a", newline="") as file:
        writer = csv.writer(file)
        writer.writerow([data["value"], data["unit"], data["sensor_id"], datetime.now()])



from pymongo import MongoClient

def store_in_mongo(data):
    data["timestamp"] = datetime.now()
    client = MongoClient("mongodb://localhost:27017/")
    db = client["sensor_data"]
    collection = db["humidity_readings"]
    collection.insert_one(data)
    print("✅ Stored humidity in MongoDB:", data)
    client.close()



from py2neo import Graph, Node

def store_in_neo4j(data):
    graph = Graph("bolt://localhost:7687", auth=("neo4j", "test1234"))
    node = Node("AirQuality", status=data["status"], sensor_id=data["sensor_id"], timestamp=str(datetime.now()))
    graph.create(node)
    print("✅ Stored air quality in Neo4j:", data)



client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect(broker, port, 60)
client.loop_forever()
