from MongoDBConfig import MongoDBConfig

class NoSqlConfig:


    def __init__(self):
        self.client = MongoDBConfig().getInstance()

    def __del__(self):
        self.client.close()

    def getCollection(self, name):
        return self.client['veri_mei'][name]