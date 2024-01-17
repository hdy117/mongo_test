import pymongo
import glog


class MongoConn:
    def __init__(self, server_ip: str = "172.24.192.147", port: str = "27017"):
        self.server_ip_: str = server_ip
        self.port_: str = port
        self.client: pymongo.MongoClient = None
        self.db_names: [str] = []
        self.used_db_name: str = "employee"
        self.used_db: pymongo.mongo_client.database.Database = None

    def __del__(self):
        if self.client:
            self.client.close()

    def connect_mongo(self) -> bool:
        glog.info("=============================")
        self.client = pymongo.MongoClient('mongodb://{}:{}'.format(self.server_ip_, self.port_))

        if self.client:
            glog.info("mongo connected to {}:{}".format(self.server_ip_, self.port_))
        else:
            glog.info("mongo fail to connect to {}:{}".format(self.server_ip_, self.port_))

        return self.client is not None

    def list_db_names(self):
        glog.info("=============================")
        if self.client:
            self.db_names = self.client.list_database_names()
            glog.info("db_names:{}".format(self.db_names))

    def select_db(self, db_name: str):
        glog.info("=============================")
        if self.client:
            self.used_db_name = db_name
            self.used_db = self.client[self.used_db_name]
            collection_names = self.used_db.list_collection_names()
            glog.info("collection_names:{}".format(collection_names))

    def insert_document(self, dict_data: dict, collection_name: str):
        glog.info("=============================")
        if self.client is not None and self.used_db is not None:
            collection = self.used_db[collection_name]
            ret = collection.insert_one(dict_data)
            glog.info("ret:{}".format(ret.inserted_id))
            docs = collection.find()
            for doc in docs:
                glog.info("doc keys:{}".format(doc.keys()))

    def find_content(self, dict_data: dict, collection_name: str):
        glog.info("=============================")
        if self.client is not None and self.used_db is not None:
            collection = self.used_db[collection_name]
            docs = collection.find()
            for doc in docs:
                glog.info("doc keys:{}".format(doc.keys()))
                if "report" in doc.keys():
                    glog.info("doc[report] keys:{}".format(doc["report"].keys()))
                    glog.info("length of doc[report][cases]:{}".format(len(doc["report"]["cases"])))

    def find_content_filter(self, dict_data: dict, collection_name: str):
        glog.info("=============================")
        if self.client is not None and self.used_db is not None:
            collection = self.used_db[collection_name]
            # only enable report find
            docs = collection.find({}, {"report": 1})
            for doc in docs:
                glog.info("doc keys:{}".format(doc.keys()))
                if "report" in doc.keys():
                    glog.info("doc[report] keys:{}".format(doc["report"].keys()))
                    glog.info("length of doc[report][cases]:{}".format(len(doc["report"]["cases"])))
