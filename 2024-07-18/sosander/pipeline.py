from pymongo import MongoClient, errors
import logging
from settings import MONGO_URI, MONGODB_NAME, MONGO_COLLECTION_URL,MONGO_COLLECTION_CATEGORY,MONGO_COLLECTION_DATA,MONGO_COLLECTION_CRAWLER_ERROR,MONGO_COLLECTION_PARSER_ERROR

class MongoPipeline:
    def __init__(self):
        self.client = MongoClient(MONGO_URI)
        self.db = self.client[MONGODB_NAME]
        
        try:
            self.db[MONGO_COLLECTION_URL].create_index("product_url", unique=True)
            self.db[MONGO_COLLECTION_CRAWLER_ERROR].create_index("error_url", unique=True)
            self.db[MONGO_COLLECTION_CATEGORY].create_index("subcategory_url", unique=True)
            self.db[MONGO_COLLECTION_DATA].create_index("url", unique=True)
            self.db[MONGO_COLLECTION_PARSER_ERROR].create_index("failed_url", unique=True)
            
        except errors.PyMongoError as e:
            logging.warning(e)