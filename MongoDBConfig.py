import pymongo


class MongoDBConfig:
    __instance = None

    @staticmethod
    def getInstance():
        if MongoDBConfig.__instance is None:
            MongoDBConfig()
        return MongoDBConfig.__instance

    def __init__(self):
        if MongoDBConfig.__instance is not None:
            raise Exception("This class is a singleton! Mongo")
        else:
            MongoDBConfig.__instance = pymongo.MongoClient("LINK MONGODB")
            