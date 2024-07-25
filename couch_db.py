import json
import os
import couchdb
from dotenv import load_dotenv
from redis import StrictRedis

load_dotenv()

class BaseDB:
    def __init__(self):
        self.db_class = CouchDB()

    def insert(self, key, data):
        self.db_class.insert(key, data)

    def retrieve(self, key):
        return self.db_class.retrieve(key)

    def delete_doc(self, key):
        return self.db_class.delete_doc(key)

    def add_one(self, key, data, sort_key, is_sort=False, desc=False):
        self.db_class.add_one(key, data, sort_key, is_sort, desc)
    
    def add_all(self, key, data_all, sort_key, is_sort=False, desc=False):
        self.db_class.add_all(key, data_all, sort_key, is_sort, desc)

    def replace_all(self, key, data_all, sort_key, is_sort=False, desc=False):
        self.db_class.replace_all(key, data_all, sort_key, is_sort, desc)

    def get_doc_ids(self, db):
        return self.db_class.get_doc_ids(db)


class CouchDB:
    def __init__(self):
        self.connect()

    def connect(self):
        self.client = couchdb.Server(os.environ.get("COUCH_URL"))

    def insert(self, key, data):
        db_name, doc_id = key.split("/")[0], key.split("/")[1]
        if db_name in self.client:
            db = self.client[db_name]
        else:
            db = self.client.create(db_name)
        ex_doc = db.get(doc_id)
        if ex_doc:
            try:
                db.save({"_id": doc_id, "_rev": ex_doc["_rev"], "data": data})
            except couchdb.http.ResourceConflict:
                print("Could not update. Document update conflict.")
        else:
            db.save({"_id": doc_id, "data": data})
    
    def retrieve(self, key):
        db_name, doc_id = key.split("/")[0], key.split("/")[1]
        if db_name not in self.client:
            return []
        db = self.client[db_name]
        db_data = db.get(doc_id, default={"data": []})
        return db_data["data"]

    def delete_doc(self, key):
        db_name, doc_id = key.split("/")[0], key.split("/")[1]
        if db_name not in self.client:
            return False
        db = self.client[db_name]
        try:
            doc = db[doc_id]
            db.delete(doc)
            return True
        except KeyError:
            return False

    def add_one(self, key, data, sort_key, is_sort=False, desc=False):
        db_data = self.retrieve(key)
        av_data = [d[sort_key] for d in db_data]
        if data[sort_key] not in av_data: 
            db_data.append(data)
        if is_sort:
            db_data.sort(key=lambda x:x[sort_key], reverse=desc)
        self.insert(key, db_data)

    def add_all(self, key, data_list, sort_key, is_sort=True, desc=False):
        c=0
        db_data = self.retrieve(key)
        av_data = [d[sort_key] for d in db_data]
        for data in data_list:
            if data[sort_key] not in av_data: 
                db_data.append(data)
                c+=1
        if is_sort:
            db_data.sort(key=lambda x:x[sort_key], reverse=desc)
        self.insert(key, db_data)

    def replace_all(self, key, data_list, sort_key, is_sort=False, desc=False):
        c=0
        db_data = [d for d in data_list]
        if is_sort:
            db_data.sort(key=lambda x:x[sort_key], reverse=desc)
        self.insert(key, db_data)

    def get_doc_ids(self, db):
        if db not in self.client:
            return []
        return [id for id in self.client[db]]
    
class RedisDB:
    def __init__(self):
        self.db_name = "3"
        self.connect()

    def connect(self):
        self.client = StrictRedis(db=int(self.db_name), host=os.environ.get("REDIS_HOST"), port=os.environ.get("REDIS_PORT"), password=os.environ.get("REDIS_PW"))

    def insert(self, key, data):
        self.client.set(key, json.dumps(data))

    def retrieve(self, key):
        return json.loads(self.client.get(key))