from elasticsearch import Elasticsearch
import time
class ElasticsearchClient:
    def __init__(self, host="localhost", port="9200"):
        self.host = host
        self.port = port
        self.client = Elasticsearch(["http://"+self.host+":"+self.port])

        while not self.ping():
            print("Waiting for Elasticsearch to start...")
            time.sleep(1)
        
        index_name = "sensors"
        is_index_exists = self.checkIfIndexExists(index_name=index_name)
        if not is_index_exists:
            self.create_index(index_name)
            mapping = {
                "properties": {
                    "name": {"type": "keyword"},
                    "type": {"type": "text"},
                    "description": {"type": "text"}
                }
            }
            self.create_mapping(index_name, mapping)

    def ping(self):
        return self.client.ping()
    
    def clearIndex(self, index_name):
        if self.client.indices.exists(index=index_name):
            # If the index exists, delete it
            return self.client.indices.delete(index=index_name)
        else:
            # If the index does not exist, do nothing
            return None
    
    def close(self):
        self.client.close()

    def create_index(self, index_name):
        return self.client.indices.create(index=index_name)
    
    def create_mapping(self, index_name, mapping):
        return self.client.indices.put_mapping(index=index_name, body=mapping)
    
    def search(self, index_name, query):
        return self.client.search(index=index_name, body=query)
    
    def index_document(self, index_name, document):
        return self.client.index(index=index_name, document=document)
    
    def checkIfIndexExists(self, index_name):
        return self.client.indices.exists(index=index_name)
    
    
    
