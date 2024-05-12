from pymongo import MongoClient

class MongoDBClient:
    def __init__(self, host="localhost", port=27017):
        self.host = host
        self.port = port
        self.client = MongoClient(host, port)
        self.database = None
        self.collection = None

    def close(self):
        self.client.close()
    
    def ping(self):
        return self.client.db_name.command('ping')
    
    def getDatabase(self, database):
        self.database = self.client[database]
        return self.database

    def getCollection(self, collection):
        self.collection = self.database[collection]
        return self.collection
    
    def clearDb(self,database):
        self.client.drop_database(database)

    def insertOne(self, doc):
        x = self.collection.insert_one(doc)
        return x
    
    def findOne(self, query={}):
        x = self.collection.find_one(query)
        return x

    def deleteOne(self, query={}):
        x = self.collection.delete_one(query)
        return x
    
    def findAllDocuments(self, query={}):
        return self.collection.find(query)
