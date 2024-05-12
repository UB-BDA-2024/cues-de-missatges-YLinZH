from fastapi import HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional
from app.redis_client import RedisClient
from app.mongodb_client import MongoDBClient
from app.elasticsearch_client import ElasticsearchClient 
from app.timescale import Timescale
from app.cassandra_client import CassandraClient
import json

from . import models, schemas

def get_sensor(db: Session, sensor_id: int) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()

def get_sensor_by_name(db: Session, name: str) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.name == name).first()

def get_sensors(db: Session, skip: int = 0, limit: int = 100) -> List[models.Sensor]:
    return db.query(models.Sensor).offset(skip).limit(limit).all()


def create_sensor(db: Session, sensor: schemas.SensorCreate, mongodb_client: MongoDBClient, es: ElasticsearchClient):
    db_sensor = models.Sensor(name=sensor.name)
    mongodb_client.getDatabase("sensors")
    mongodb_client.getCollection("sensorsCol")
    db.add(db_sensor)
    db.commit()
    db.refresh(db_sensor)

    mydoc = {
        "sensor_id": db_sensor.id,
        "location": {
            "type": "Point",
            "coordinates": [sensor.longitude, sensor.latitude]
        },
        "type": sensor.type,
        "mac_address": sensor.mac_address,
        "manufacturer": sensor.manufacturer,
        "model": sensor.model,
        "serie_number": sensor.serie_number,
        "firmware_version": sensor.firmware_version,
        "description": sensor.description
        }

    mongodb_client.insertOne(mydoc)
    
    x = get_sensor_document(sensor_id=db_sensor.id, mongodb_client=mongodb_client)
    if x is None:
        raise HTTPException(status_code=404, detail="Sensor not found") 
    es_doc = {
        "name": sensor.name,
        "type": x["type"],
        "description": x["description"]
    }

    index_name = "sensors"
    condition = {
        "query": {
            "match": {
                "name": sensor.name
            }
        }
    }
    results = es.search(index_name,condition)
    if results["hits"]["hits"] == []:
        es.index_document(index_name, es_doc)

    return_sensor = {
        "id": db_sensor.id, 
        "name": sensor.name,
        "latitude": x["location"]["coordinates"][0],
        "longitude": x["location"]["coordinates"][1],
        "type": sensor.type,
        "mac_address": sensor.mac_address,
        "manufacturer": sensor.manufacturer,
        "model": sensor.model,
        "serie_number": sensor.serie_number,
        "firmware_version": sensor.firmware_version,
        "description": sensor.description
    }
    return return_sensor

def record_data(redis: RedisClient, sensor_id: int, data: schemas.SensorData, db: Session, mongodb_client: MongoDBClient, ts: Timescale, cassandra: CassandraClient) -> schemas.Sensor:
    #obtenir sensor si existeix
    db_sensor = get_sensor(db, sensor_id)
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    serialized_data = json.dumps(data.dict())

    sensor_document = get_sensor_document(sensor_id=sensor_id, mongodb_client=mongodb_client)
    if sensor_document is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    
    # guardar noves dades en la base de dades
    redis.set(sensor_id, serialized_data)

    velocity = "NULL"
    humidity = "NULL"
    temperature = "NULL"

    if data.temperature and data.humidity:
        temperature = data.temperature
        humidity = data.humidity
    else:
        velocity = data.velocity
    query = f"""
            INSERT INTO sensor_data (time, sensor_id, velocity, temperature, humidity, battery_level) 
            VALUES ('{data.last_seen}', {sensor_id}, {velocity}, {temperature}, {humidity}, {data.battery_level})
        """

    # Executa la consulta SQL
    ts.execute(query)

    # Confirma els canvis
    ts.conn.commit()

    # guardar les dades a cassandra

    # primera pregunta
    if (temperature != "NULL"):
        cassandra.execute(f"""
            INSERT INTO temperature_data (sensor_id, temperature, time)
            VALUES ({sensor_id},{temperature}, '{data.last_seen}')
        """)

    # segunda pregunta
    cassandra.execute(f"""        
        INSERT INTO sensor_count_by_type (sensor_id, sensor_type, time)
        VALUES ({sensor_id}, '{sensor_document["type"]}', '{data.last_seen}')
    """)

    #tercera pregunta
    if (data.battery_level < 0.2):
        cassandra.execute(f"""        
            INSERT INTO low_battery_sensors (sensor_id, battery_level, time)
            VALUES ({sensor_id}, {data.battery_level}, '{data.last_seen}')
        """)

    sensor = schemas.Sensor(id=sensor_id, name=db_sensor.name, 
                            latitude=sensor_document["location"]["coordinates"][0], longitude=sensor_document["location"]["coordinates"][1],
                            type=sensor_document["type"], mac_address=sensor_document["mac_address"],
                            joined_at=db_sensor.joined_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"), temperature=data.temperature,
                            velocity=data.velocity, humidity=data.humidity, battery_level=data.battery_level, last_seen=data.last_seen, description=sensor_document["description"])
    return sensor

def get_data(from_time: str, to: str, bucket: str, redis: RedisClient, sensor_id: int, db: Session, mongodb_client: MongoDBClient, ts: Timescale):
    
    # #obtenir les dades des de PostgreSQL i Redis si existeixen
    db_sensordata_bytes = redis.get(sensor_id)
    if db_sensordata_bytes is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    db_sensor = get_sensor(db, sensor_id)
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    
    # obtenir les dades de Redis
    db_sensordata = schemas.SensorData.parse_raw(db_sensordata_bytes)
    sensor_document = get_sensor_document(sensor_id=sensor_id, mongodb_client=mongodb_client)

    query = f"""
        SELECT
            time_bucket('1{bucket}', time) AS bucket_time,
            AVG(velocity) AS avg_velocity,
            AVG(temperature) AS avg_temperature,
            AVG(humidity) AS avg_humidity,
            AVG(battery_level) AS avg_battery_level
        FROM
            sensor_data
        WHERE
            sensor_id = {sensor_id} AND
            time >= '{from_time}' AND
            time <= '{to}'
        GROUP BY
            bucket_time
        ORDER BY
            bucket_time;
    """

    ts.execute(query)


    # Fetch the results
    results = ts.cursor.fetchall()

    return results

def get_sensor_document(sensor_id: int, mongodb_client: MongoDBClient):
    mongodb_client.getDatabase("sensors")
    mongodb_client.getCollection("sensorsCol")
    x = mongodb_client.findOne({"sensor_id": sensor_id})
    # convert ObjectId to string for JSON serialization
    if x:
        x["_id"] = str(x["_id"])
        return x
    return None


def get_sensors_near(latitude: float, longitude: float, radius: int, db: Session, mongodb_client: MongoDBClient, redis_client: RedisClient):
    mongodb_client.getDatabase("sensors")
    collection = mongodb_client.getCollection("sensorsCol")
    collection.create_index([("location", "2dsphere")])
    geoJSON = {
        "location": {
            "$near": {
                "$geometry": {
                    "type": "Point",
                    "coordinates": [longitude, latitude]
                },
                "$maxDistance": radius
            }
        }
    }
    result = mongodb_client.findAllDocuments(geoJSON)
    # Convert MongoDB cursor to a list of dictionaries
    nearby_sensors = list(result)
    sensors = []
    # Optionally, convert ObjectId to string for JSON serialization
    for doc in nearby_sensors:
        doc["_id"] = str(doc["_id"])
        sensor = get_data(redis=redis_client, sensor_id=doc["sensor_id"], db=db, mongodb_client=mongodb_client)
        if sensor is not None:
            sensors.append(sensor)
    
    if sensors is not None:
        return sensors
    return []

def delete_sensor(db: Session, sensor_id: int, mongodb_client: MongoDBClient):
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    
    mongodb_client.getDatabase("sensors")
    mongodb_client.getCollection("sensorsCol")
    mongodb_client.deleteOne({"sensor_id": sensor_id})

    db.delete(db_sensor)
    db.commit()
    return db_sensor

def get_sensor_without_redis(sensor_id: int, db: Session, mongodb_client: MongoDBClient):
    # get PostgreSQL sensor data
    db_sensor = get_sensor(db, sensor_id)
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    
    # get MongoDB sensor data
    x = get_sensor_document(sensor_id=sensor_id, mongodb_client=mongodb_client)
    if x is None:
        raise HTTPException(status_code=404, detail="Sensor not found") 

    return_sensor = {
        "id": db_sensor.id,
        "name": db_sensor.name,
        "latitude": x["location"]["coordinates"][0],
        "longitude": x["location"]["coordinates"][1],
        "type": x["type"],
        "mac_address": x["mac_address"],
        "manufacturer": x["manufacturer"],
        "model": x["model"],
        "serie_number": x["serie_number"],
        "firmware_version": x["firmware_version"],
        "description": x["description"]
    }

    return return_sensor


def search_sensors(db: Session, mongodb_client: MongoDBClient, es: ElasticsearchClient, query: str, size: int, search_type: str):

    query_json = json.loads(query)
    condition = {}
    field = ""
    for i in query_json.keys():
        field = i
        break

    if search_type == "match":
        condition = {
            "query": {
                "match": query_json
            },
            "size": size
        }
    elif search_type == "similar":
        condition = {
            "query": {
                "fuzzy": {
                    field: {
                        "value": query_json[field],
                        "fuzziness": "AUTO",
                    }
                }
            },
            "size": size 
        }

    elif search_type == "prefix":
        condition = {
            "query": {
                "prefix": {
                    field: {
                        "value": query_json[field],
                    }
                }
            },
            "size": size
        }

    # Elasticsearch
    index_name = "sensors"

    is_index_exists = es.checkIfIndexExists(index_name=index_name)
    if not is_index_exists:
        raise HTTPException(status_code=404, detail="Index not found")
    results = es.search(index_name, condition)
    hits = results["hits"]["hits"]

    es_list = []

    for hit in hits:

        # get postgresql data
        db_sensor = get_sensor_by_name(db, hit['_source']['name'])
        if db_sensor is None:
            raise HTTPException(status_code=404, detail="Sensor not found")
        

        # get MongoDB sensor data
        x = get_sensor_document(sensor_id=db_sensor.id, mongodb_client=mongodb_client)
        if x is None:
            raise HTTPException(status_code=404, detail="Sensor not found") 
        
        # merge the sensor datas
        sensor = {
            "id": db_sensor.id,
            "name": hit['_source']['name'],
            "latitude": x["location"]["coordinates"][0],
            "longitude": x["location"]["coordinates"][1],
            "type": hit['_source']["type"],
            "mac_address": x["mac_address"],
            "manufacturer": x["manufacturer"],
            "model": x["model"],
            "serie_number": x["serie_number"],
            "firmware_version": x["firmware_version"],
            "description": hit['_source']["description"]
        }
        es_list.append(sensor)

    return es_list

def get_temperature_values(db: Session, mongodb_client: MongoDBClient, cassandra: CassandraClient):
    sensors = []
    
    rows = cassandra.execute("""
        SELECT
            sensor_id,
            MAX(temperature) AS max_temperature,
            MIN(temperature) AS min_temperature,
            AVG(temperature) AS average_temperature
        FROM
            sensor.temperature_data
        GROUP BY
            sensor_id
        """)
    
    for row in rows:
        sensor_postgres = get_sensor(db=db, sensor_id=row[0])
        sensor_document = get_sensor_document(sensor_id=row[0], mongodb_client=mongodb_client)
        if sensor_document is None:
            raise HTTPException(status_code=404, detail="Sensor not found")
        sensors.append({
            "id": row[0],
            "name": sensor_postgres.name,
            "latitude": sensor_document["location"]["coordinates"][0],
            "longitude": sensor_document["location"]["coordinates"][1],
            "type": sensor_document["type"],
            "mac_address": sensor_document["mac_address"],
            "manufacturer": sensor_document["manufacturer"],
            "model": sensor_document["model"],
            "serie_number": sensor_document["serie_number"],
            "firmware_version": sensor_document["firmware_version"],
            "description": sensor_document["description"],
            "values": [
                {
                    "max_temperature": row[1], 
                    "min_temperature": row[2], 
                    "average_temperature": row[3]
                }
            ]
            })
    return {"sensors": sensors}

def get_sensors_quantity(cassandra: CassandraClient):
    sensors = []
    rows = cassandra.execute("""
        SELECT
            sensor_type, COUNT(sensor_type) AS quantity
        FROM
            sensor.sensor_count_by_type
        GROUP BY
            sensor_type
        """)
    
    for row in rows:
        sensors.append({
            "type" : row[0],
            "quantity" : row[1]
        })

    return {"sensors": sensors}

def get_low_battery_sensors(db: Session, mongodb_client: MongoDBClient, cassandra: CassandraClient):
    sensors = []
    rows = cassandra.execute("""
        SELECT
            sensor_id, battery_level
        FROM
            sensor.low_battery_sensors
        """)
    
    for row in rows:
        sensor_postgres = get_sensor(db=db, sensor_id=row[0])
        sensor_document = get_sensor_document(sensor_id=row[0], mongodb_client=mongodb_client)
        if sensor_document is None:
            raise HTTPException(status_code=404, detail="Sensor not found")
        sensors.append({
            "id": row[0],
            "name": sensor_postgres.name,
            "latitude": sensor_document["location"]["coordinates"][0],
            "longitude": sensor_document["location"]["coordinates"][1],
            "type": sensor_document["type"],
            "mac_address": sensor_document["mac_address"],
            "manufacturer": sensor_document["manufacturer"],
            "model": sensor_document["model"],
            "serie_number": sensor_document["serie_number"],
            "firmware_version": sensor_document["firmware_version"],
            "description": sensor_document["description"],
            "battery_level": row[1]
            })

    return {"sensors": sensors}