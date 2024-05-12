from cassandra.cluster import Cluster

class CassandraClient:
    def __init__(self, hosts):
        self.cluster = Cluster(hosts,protocol_version=4)

        self.session = self.cluster.connect()

        self.create_keyspace()

        self.session.set_keyspace("sensor")
        # # Crear taules
        self.create_tables()
        
    def get_session(self):
        return self.session

    def close(self):
        self.cluster.shutdown()

    def execute(self, query):
        return self.get_session().execute(query)

    def create_keyspace(self):
        self.session.execute("""
            CREATE KEYSPACE IF NOT EXISTS sensor WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
        """)
    
    def create_tables(self):
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS sensor.temperature_data (
                sensor_id INT,
                temperature FLOAT,  
                time TIMESTAMP,
                PRIMARY KEY (sensor_id, time)
            )
        """)
        
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS sensor.sensor_count_by_type (
                sensor_id INT,
                sensor_type TEXT,
                time TIMESTAMP,
                PRIMARY KEY (sensor_type, sensor_id)
            )
        """)
        
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS sensor.low_battery_sensors (
                sensor_id INT,
                battery_level DECIMAL,
                time TIMESTAMP,
                PRIMARY KEY (sensor_id, time)
            )
        """)