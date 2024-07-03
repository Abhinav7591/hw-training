from pymongo import MongoClient
import logging

uri = "mongodb://localhost:27017/"
db_name = "carrefouruae_2024_07_02" 

class MongoPipeline:
    def __init__(self):
        self.client = MongoClient(uri)
        self.db = self.client[db_name]
        try:
            self.db["carrefouruae_product_urls"].create_index("url", unique=True)
            self.db["carrefouruae_product_data"].create_index("Url", unique=True)
            self.db["carrefouruae_category_urls"].create_index("sub_category_url", unique=True)
        except Exception as e:
            logging.warning(e)