import os
import sys
import json

import glog

abs_dir_path = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(abs_dir_path, ".."))
data_dir = os.path.join(abs_dir_path, "../data")

from mongo_test import mongo_conn


def get_dict_file(folder: str) -> str:
    files = os.listdir(folder)
    for file in files:
        if file.endswith(".dict"):
            json_file = os.path.join(folder, file)
            return json_file
    return ""


def load_json_from_file(json_file: str):
    json_loaded = {}
    with open(json_file, 'r', encoding="utf-8") as json_fd:
        json_loaded = json.load(json_fd)

    return json_loaded


def mongo_test():
    mongo = mongo_conn.MongoConn()
    mongo.connect_mongo()
    mongo.list_db_names()
    mongo.select_db("employee")

    # get json file from folder
    json_file = get_dict_file(data_dir)
    glog.info("json_file:{}".format(json_file))
    dict_data = load_json_from_file(json_file)
    glog.debug("dict_data:{}".format(dict_data))

    # insert into collection
    # mongo.insert_document(dict_data, "department")
    mongo.find_content(dict_data, "department")
    mongo.find_content_filter(dict_data, "department")


if __name__ == "__main__":
    mongo_test()
