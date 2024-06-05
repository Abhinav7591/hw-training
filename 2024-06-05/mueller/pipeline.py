from pymongo import MongoClient
import logging


def get_mongo_client():
    try:
        client = MongoClient("mongodb://localhost:27017/")
        logging.info("Connected to MongoDB successfully.")
        return client
    except Exception as e:
        logging.error("Failed to connect to MongoDB: %s", e)
        return None


def get_mongo_collection(db_name, collection_name):
    client = get_mongo_client()
    if client:
        db = client[db_name]
        collection = db[collection_name]
        collection.create_index("url", unique=True)
        return collection
    else:
        return None


def product_details_collection(db_name, collection_name):
    client = get_mongo_client()
    if client:
        db = client[db_name]
        collection = db[collection_name]
        collection.create_index("pdp_url", unique=True)
        return collection
    else:
        return None