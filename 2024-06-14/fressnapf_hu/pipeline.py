from pymongo import MongoClient
import logging

uri = "mongodb://localhost:27017/"
db_name = "fressnapf_hu"  #fressnapf_hungry

class MongoPipeline:
    def __init__(self):
        self.client = MongoClient(uri)
        self.db = self.client[db_name]
        try:
            self.db["product_urls"].create_index("url", unique=True)
            self.db["product_details"].create_index("pdp_url", unique=True)
        except Exception as e:
            logging.warning(e)