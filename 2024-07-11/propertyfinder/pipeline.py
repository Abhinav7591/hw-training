from pymongo import MongoClient, errors
import logging

uri = "mongodb://localhost:27017/"
db_name = "propertyfinder_2024_07_11" 

class MongoPipeline:
    def __init__(self):
        self.client = MongoClient(uri)
        self.db = self.client[db_name]
        try:
            
            self.db["Propertyfinder_property_url"].create_index("property_url", unique=True)
            self.db["Propertyfinder_crawler_error_urls"].create_index("error_url", unique=True)
            self.db["Propertyfinder_category_urls"].create_index("subcategory_url", unique=True)
            self.db["Propertyfinder_property_data"].create_index("url", unique=True) 
            self.db["Propertyfinder_parser_error_urls"].create_index("failed_url", unique=True)         
        except errors.PyMongoError as e:
            logging.warning(e)