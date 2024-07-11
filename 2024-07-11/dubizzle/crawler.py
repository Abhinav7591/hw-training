from pipeline import MongoPipeline
from settings import headers, base_url,cookies
from parsel import Selector
import requests
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DubizzleCrawler:
    def __init__(self):
        self.headers = headers
        self.mongo_pipeline = MongoPipeline()
        self.collection = self.mongo_pipeline.db["dubizzle_property_ul"]
        self.error_collection = self.mongo_pipeline.db["dubizzle_crawler_error_urls"]
        self.category_collection = self.mongo_pipeline.db["dubizzle_category_urls"]
        
    def fetch_subcategory_urls(self):
        subcategory_urls = self.category_collection.distinct("subcategory_url")
        return subcategory_urls

    def crawl_and_store_urls(self):
        subcategory_urls = self.fetch_subcategory_urls()

        for subcategory_url in subcategory_urls:
            page_number = 1
            while True:
                next_page = f"{subcategory_url}?page={page_number}"
                response = requests.get(next_page, headers=self.headers, cookies=cookies)
                
                if response.status_code == 200:
                    selector = Selector(response.text)
                    property_urls = selector.xpath("//div[@id='listing-card-wrapper']//a[@type='property']/@href").extract()
                    if not property_urls:
                        logging.info(f"No property URLs found on page {page_number} for {subcategory_url}.")
                        break  
                    
                    for property_url in property_urls:
                        full_url = base_url + property_url
                        try:
                            self.collection.insert_one({'subcategory_url': subcategory_url, 'property_url': full_url})
                            logging.info(f"Inserted URL: {full_url}")
                        except Exception as e:
                            logging.error(f"Error inserting URL {full_url}: {e}")
                            
                    page_number += 1
                else:
                    try:
                        self.error_collection.insert_one({'error_url': next_page, 'status_code': response.status_code})
                        logging.error(f"Error fetching {next_page}. Status Code: {response.status_code}")
                    except Exception as e:
                        logging.error(f"Error inserting error URL {next_page} Status Code: {response.status_code}: {e}")
                    break  

if __name__ == '__main__':
    crawler = DubizzleCrawler()
    crawler.crawl_and_store_urls()
