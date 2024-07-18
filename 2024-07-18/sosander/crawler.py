from pipeline import MongoPipeline
from settings import HEADERS
from parsel import Selector
import requests
import logging

class Crawler:
    def __init__(self):
        self.headers = HEADERS
        self.mongo_pipeline = MongoPipeline()
        self.collection = self.mongo_pipeline.db["sosander_product_url"]
        self.error_collection = self.mongo_pipeline.db["sosander_crawler_error_urls"]
        self.category_collection = self.mongo_pipeline.db["sosander_category_urls"]
        
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        
    def fetch_subcategory_urls(self):
        subcategory_urls = self.category_collection.distinct("subcategory_url")
        return subcategory_urls

    def crawl(self):
        subcategory_urls = self.fetch_subcategory_urls()

        for subcategory_url in subcategory_urls:
            page_number = 1
            while True:
                next_page = f"{subcategory_url}?p={page_number}"
                response = requests.get(next_page, headers=self.headers)
                
                if response.status_code == 200:
                    selector = Selector(response.text)
                    product_urls = selector.xpath("//a[@class ='product photo-rollover']/@href").extract()
                    if not product_urls:
                        logging.info(f"No product URLs found on page {page_number} for {subcategory_url}.")
                        break  
                    
                    for product_url in product_urls:
                        try:
                            self.collection.insert_one({'subcategory_url': subcategory_url, 'product_url': product_url})
                            logging.info(f"Inserted URL: {product_url}")
                        except Exception as e:
                            logging.error(f"Error inserting URL {product_url}: {e}")
                            
                    page_number += 1
                else:
                    try:
                        self.error_collection.insert_one({'error_url': next_page, 'status_code': response.status_code})
                        logging.error(f"Error fetching {next_page}. Status Code: {response.status_code}")
                    except Exception as e:
                        logging.error(f"Error inserting error URL {next_page} Status Code: {response.status_code}: {e}")
                    break  

if __name__ == '__main__':
    crawler = Crawler()
    crawler.crawl()
