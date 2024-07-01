from pymongo import MongoClient
import logging

uri = "mongodb://localhost:27017/"
db_name = "luluhypermarket_2024_07_01" 

class MongoPipeline:
    def __init__(self):
        self.client = MongoClient(uri)
        self.db = self.client[db_name]
        try:
            self.db["luluhypermarket_product_urls"].create_index("url", unique=True)
            self.db["luluhypermarket_product_data"].create_index("Url", unique=True)
        except Exception as e:
            logging.warning(e)