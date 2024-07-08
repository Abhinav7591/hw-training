from pymongo import MongoClient, errors
import logging

uri = "mongodb://localhost:27017/"
db_name = "dubizzle_2024_07_09" 

class MongoPipeline:
    def __init__(self):
        self.client = MongoClient(uri)
        self.db = self.client[db_name]
        try:
            self.db["dubizzle_category_urls"].create_index("subcategory_url", unique=True)
            self.db["dubizzle_property_urls"].create_index("property_url", unique=True)
            self.db["dubizzle_crawler_error_urls"].create_index("error_url", unique=True)
            self.db["dubizzle_property_data"].create_index("url", unique=True) 
            self.db["dubizzle_parser_error_urls"].create_index("failed_url", unique=True)         
        except errors.PyMongoError as e:
            logging.warning(e)