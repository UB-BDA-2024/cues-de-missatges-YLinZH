from fastapi.testclient import TestClient
import pytest
from app.main import app
from app.redis_client import RedisClient
from app.mongodb_client import MongoDBClient
from app.elasticsearch_client import ElasticsearchClient
from app.timescale import Timescale
from app.cassandra_client import CassandraClient
import time

client = TestClient(app)

@pytest.fixture(scope="session", autouse=True)
def clear_dbs():
     from app.database import SessionLocal, engine
     from app.sensors import models
     models.Base.metadata.drop_all(bind=engine)
     models.Base.metadata.create_all(bind=engine)
     redis = RedisClient(host="redis")
     redis.clearAll()
     redis.close()
     mongo = MongoDBClient(host="mongodb")
     mongo.clearDb("sensors")
     mongo.close()
     es = ElasticsearchClient(host="elasticsearch")
     es.clearIndex("sensors")  
     ts = Timescale()
     ts.execute("DELETE FROM sensor_data")
     #TODO execute TS migrations
     ts.execute("commit")
    #  ts.execute("DROP TABLE IF EXISTS sensor_data")
     ts.close()

     while True:
        try:
            cassandra = CassandraClient(["cassandra"])
            cassandra.get_session().execute("DROP KEYSPACE IF EXISTS sensor")
            cassandra.close()
            break
        except Exception as e:
            time.sleep(5)


def test_redis_connection():
    redis_client = RedisClient(host="redis")
    assert redis_client.ping()
    redis_client.close()


def test_mongodb_connection():
    mongodb_client = MongoDBClient(host="mongodb")
    assert mongodb_client.ping()
    mongodb_client.close()



def test_elasticsearch_client():
    """Elasticsearch client can be properly created"""
    es = ElasticsearchClient(host="elasticsearch")
    assert es.ping()
    # Create the index
    es_index_name='my_index'
    es.create_index(es_index_name)
    # Define the mapping for the index
    mapping = {
        'properties': {
            'title': {'type': 'text'},
            'description': {'type': 'text'},
            'price': {'type': 'float'}
        }
    }
    es.create_mapping('my_index',mapping)
    es_doc={
        'title': 'Test document',
        'description': 'This is a test document',
        'price': 10.0
    }
    es.index_document(es_index_name, es_doc)
    # Define the search query
    query = {
        'query': {
            'match': {
                'title': 'Test document'
            }
        }
    }
    # Perform the search and get the results
    results = es.search(es_index_name, query)

    for hit in results['hits']['hits']:
        print(f"Title: {hit['_source']['title']}")
        print(f"Price: {hit['_source']['price']}")
        assert hit['_source']['title'] == 'Test document'
        assert hit['_source']['price'] == 10.0
    
    # Delete the index
    es.clearIndex(es_index_name)
    es.close()

def test_create_sensor_temperatura_1():
    """A sensor can be properly created"""
    response = client.post("/sensors", json={"name": "Sensor Temperatura 1", "latitude": 1.0, "longitude": 1.0, "type": "Temperatura", "mac_address": "00:00:00:00:00:00", "manufacturer": "Dummy", "model":"Dummy Temp", "serie_number": "0000 0000 0000 0000", "firmware_version": "1.0", "description": "Sensor de temperatura model Dummy Temp del fabricant Dummy"})
    assert response.status_code == 200
    assert response.json() == {"id": 1, "name": "Sensor Temperatura 1", "latitude": 1.0, "longitude": 1.0, "type": "Temperatura", "mac_address": "00:00:00:00:00:00", "manufacturer": "Dummy", "model":"Dummy Temp", "serie_number": "0000 0000 0000 0000", "firmware_version": "1.0", "description": "Sensor de temperatura model Dummy Temp del fabricant Dummy"}

def test_create_sensor_velocitat_1():
    response = client.post("/sensors", json={"name": "Velocitat 1", "latitude": 1.0, "longitude": 1.0, "type": "Velocitat", "mac_address": "00:00:00:00:00:01", "manufacturer": "Dummy", "model":"Dummy Vel", "serie_number": "0000 0000 0000 0000", "firmware_version": "1.0", "description": "Sensor de velocitat model Dummy Vel del fabricant Dummy cruïlla 1"})
    assert response.status_code == 200
    assert response.json() == {"id": 2, "name": "Velocitat 1", "latitude": 1.0, "longitude": 1.0, "type": "Velocitat", "mac_address": "00:00:00:00:00:01", "manufacturer": "Dummy", "model":"Dummy Vel", "serie_number": "0000 0000 0000 0000", "firmware_version": "1.0", "description": "Sensor de velocitat model Dummy Vel del fabricant Dummy cruïlla 1"}

def test_create_sensor_velocitat_2():
    response = client.post("/sensors", json={"name": "Velocitat 2", "latitude": 2.0, "longitude": 2.0, "type": "Velocitat", "mac_address": "00:00:00:00:00:02", "manufacturer": "Dummy", "model":"Dummy Vel", "serie_number": "0000 0000 0000 0000", "firmware_version": "1.0", "description": "Sensor de velocitat model Dummy Vel del fabricant Dummy cruïlla 2"})
    assert response.status_code == 200
    assert response.json() == {"id": 3, "name": "Velocitat 2", "latitude": 2.0, "longitude": 2.0, "type": "Velocitat", "mac_address": "00:00:00:00:00:02", "manufacturer": "Dummy", "model":"Dummy Vel", "serie_number": "0000 0000 0000 0000", "firmware_version": "1.0", "description": "Sensor de velocitat model Dummy Vel del fabricant Dummy cruïlla 2"}

def test_create_sensor_temperatura_2():
    """A sensor can be properly created"""
    response = client.post("/sensors", json={"name": "Sensor Temperatura 2", "latitude": 2.0, "longitude": 2.0, "type": "Temperatura", "mac_address": "00:00:00:00:00:03", "manufacturer": "Dummy", "model":"Dummy Temp", "serie_number": "0000 0000 0000 0000", "firmware_version": "1.0", "description": "Sensor de temperatura model Dummy Temp del fabricant Dummy"})
    assert response.status_code == 200
    assert response.json() == {"id": 4, "name": "Sensor Temperatura 2", "latitude": 2.0, "longitude": 2.0, "type": "Temperatura", "mac_address": "00:00:00:00:00:03", "manufacturer": "Dummy", "model":"Dummy Temp", "serie_number": "0000 0000 0000 0000", "firmware_version": "1.0", "description": "Sensor de temperatura model Dummy Temp del fabricant Dummy"}

def test_post_sensor_data_temperatura_1():
    response = client.post("/sensors/1/data", json={"temperature": 1.0, "humidity": 1.0, "battery_level": 1.0, "last_seen": "2020-01-01T00:00:00.000Z"})
    assert response.status_code == 200

def test_post_sensor_data_temperatura_2():
    response = client.post("/sensors/1/data", json={"temperature": 4.0, "humidity": 1.0, "battery_level": 1.0, "last_seen": "2020-01-01T01:00:00.000Z"})
    assert response.status_code == 200

def test_post_sensor_data_temperatura_3():
    response = client.post("/sensors/4/data", json={"temperature": 15.0, "humidity": 1.0, "battery_level": 1.0, "last_seen": "2020-01-02T00:00:00.000Z"})
    assert response.status_code == 200

def test_post_sensor_data_temperatura_4():
    response = client.post("/sensors/4/data", json={"temperature": 17.0, "humidity": 1.0, "battery_level": 1.0, "last_seen": "2020-01-02T01:00:00.000Z"})
    assert response.status_code == 200


def test_post_sensor_data_veolicitat_1():
    response = client.post("/sensors/2/data", json={"velocity": 1.0, "battery_level": 0.1, "last_seen": "2020-01-01T00:00:00.000Z"})
    assert response.status_code == 200

def test_post_sensor_data_veolicitat_2():
    response = client.post("/sensors/3/data", json={"velocity": 15.0, "battery_level": 0.15, "last_seen": "2020-01-01T01:00:00.000Z"})
    assert response.status_code == 200

def test_get_values_sensor_temperatura():
    response = client.get("/sensors/temperature/values")
    assert response.status_code == 200
    assert response.json() == {"sensors": [{"id": 1, "name": "Sensor Temperatura 1", "latitude": 1.0, "longitude": 1.0, "type": "Temperatura", "mac_address": "00:00:00:00:00:00", "manufacturer": "Dummy", "model":"Dummy Temp", "serie_number": "0000 0000 0000 0000", "firmware_version": "1.0", "description": "Sensor de temperatura model Dummy Temp del fabricant Dummy", "values": [{"max_temperature": 4.0, "min_temperature": 1.0, "average_temperature": 2.5}]}, {"id": 4, "name": "Sensor Temperatura 2", "latitude": 2.0, "longitude": 2.0, "type": "Temperatura", "mac_address": "00:00:00:00:00:03", "manufacturer": "Dummy", "model":"Dummy Temp", "serie_number": "0000 0000 0000 0000", "firmware_version": "1.0", "description": "Sensor de temperatura model Dummy Temp del fabricant Dummy", "values": [{"max_temperature": 17.0, "min_temperature": 15.0, "average_temperature": 16.0}]}]}
def test_get_sensors_quantity():
    response = client.get("/sensors/quantity_by_type")
    assert response.status_code == 200
    assert response.json() == {"sensors": [{"type":"Temperatura", "quantity": 2}, {"type":"Velocitat", "quantity": 2}]}

def test_get_sensors_low_battery():
    response = client.get("/sensors/low_battery")
    assert response.status_code == 200
    assert response.json() == {"sensors": [{"id": 2, "name": "Velocitat 1", "latitude": 1.0, "longitude": 1.0, "type": "Velocitat", "mac_address": "00:00:00:00:00:01", "manufacturer": "Dummy", "model":"Dummy Vel", "serie_number": "0000 0000 0000 0000", "firmware_version": "1.0", "description": "Sensor de velocitat model Dummy Vel del fabricant Dummy cruïlla 1", "battery_level": 0.1}, {"id": 3, "name": "Velocitat 2", "latitude": 2.0, "longitude": 2.0, "type": "Velocitat", "mac_address": "00:00:00:00:00:02", "manufacturer": "Dummy", "model":"Dummy Vel", "serie_number": "0000 0000 0000 0000", "firmware_version": "1.0", "description": "Sensor de velocitat model Dummy Vel del fabricant Dummy cruïlla 2", "battery_level": 0.15}]}

def test_post_sensor_data_not_exists():
    response = client.post("/sensors/5/data", json={"temperature": 1.0, "humidity": 1.0, "battery_level": 1.0, "last_seen": "2020-01-01T00:00:00.000Z"})
    assert response.status_code == 404
    assert "Sensor not found" in response.text

def test_get_sensor_data_not_exists():
    response = client.get("/sensors/5/data")
    assert response.status_code == 404
    assert "Sensor not found" in response.text

def test_update_sensor_1_data():
    response = client.post("/sensors/1/data", json={"temperature": 2.0, "humidity": 2.0, "battery_level": 1.9, "last_seen": "2020-01-01T00:00:01.000Z"})
    assert response.status_code == 200

def test_update_sensor_2_data():
    response = client.post("/sensors/2/data", json={"velocity": 46.0,"battery_level": 1.9, "last_seen": "2020-01-01T00:00:01.000Z"})
    assert response.status_code == 200

def test_search_sensors_name_similar():
    """Sensors can be properly searched by name"""
    response = client.get('/sensors/search?query={"name":"Velocidad 1"}&search_type=similar')
    assert response.status_code == 200
    assert response.json() == [{"id": 2, "name": "Velocitat 1", "latitude": 1.0, "longitude": 1.0, "type": "Velocitat", "mac_address": "00:00:00:00:00:01", "manufacturer": "Dummy", "model": "Dummy Vel", "serie_number": "0000 0000 0000 0000", "firmware_version": "1.0", "description": "Sensor de velocitat model Dummy Vel del fabricant Dummy cruïlla 1"}]


def test_search_sensors_type_limit():
    """Sensors can be properly searched by type"""
    response = client.get('/sensors/search?query={"type":"Velocitat"}&size=1')
    assert response.status_code == 200
    assert len(response.json()) == 1

def test_delete_sensor_1():
    response = client.delete("/sensors/1")
    assert response.status_code == 200

def test_delete_sensor_2():
    response = client.delete("/sensors/2")
    assert response.status_code == 200