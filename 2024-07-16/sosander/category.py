from pipeline import MongoPipeline
from settings import HEADERS, BASE_URL, CATEGORY_URL
from parsel import Selector
import requests
import logging

class Category:
    def __init__(self):
        self.pipeline = MongoPipeline()
        self.collection = self.pipeline.db["sosander_category_urls"]
        self.headers = HEADERS
        self.base_url = BASE_URL
        self.category_url = CATEGORY_URL

        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    def scrape_category(self):
        response = requests.get(self.base_url, headers=self.headers)
        
        if response.status_code == 200:
            selector = Selector(response.text)
            
            subcategory_urls = selector.xpath("//li/a/@href").extract()
            subcategory_names = selector.xpath("//li/a/span/text()").extract()

            data = []
            for sub_url,sub_name in zip(subcategory_urls,subcategory_names):
                if 'clothing' in sub_url and sub_url != self.category_url:
                    logging.info(f"Scraping sub category URL: {sub_url}")
                    response = requests.get(sub_url, headers=self.headers)
                    
                    if response.status_code == 200:
                        sub_selector = Selector(response.text)
                        product_count = sub_selector.xpath("//p[@class ='toolbar-amount']/span[3]/text()").extract_first()
                        if not product_count:
                            product_count = sub_selector.xpath("//p[@class ='toolbar-amount']/span/text()").extract_first()
                        
                        data.append({
                            'category_url': self.category_url,
                            'subcategory_url': sub_url,
                            'subcategory_name' : sub_name,
                            'product_count': product_count                          
                        })
                        
                        logging.info(f"Found {product_count} products in {sub_url}")
                    else:
                        logging.error(f"Failed to retrieve data from sub category url: {self.sub_url}")

            self.save_to_mongo(data)
            
            logging.info("Scraping and saving process completed.")
        else:
            logging.error(f"Failed to retrieve data from base url: {self.base_url}")
    
    def save_to_mongo(self, data):
        for item in data:
            try:
                self.collection.insert_one(item)
                logging.info(f"Stored {item['subcategory_url']} to MongoDB")
            except Exception as e:
                logging.error(f"Failed to store {item['subcategory_url']} to MongoDB: {e}")
                
if __name__ == "__main__":
    scraper = Category()
    scraper.scrape_category()