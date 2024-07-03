from settings import headers_category, base_url, params_category,parser_header
from pipeline import MongoPipeline
from parsel import Selector
import requests
import logging

class CarrefourCategory:
    def __init__(self):
        self.base_url = base_url
        self.headers = headers_category
        self.params = params_category
        self.mongo_pipeline = MongoPipeline()
        self.collection_category = self.mongo_pipeline.db["carrefouruae_category_urls"]
        
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    def fetch_menu(self):
        response = requests.get('https://www.carrefouruae.com/api/v1/menu', params=self.params, headers=self.headers)
        return response.json()

    def crawl_data(self, data):
        if data:
            for category1 in data:
                children1 = category1.get('children', [])
                for category2 in children1:
                    main_url = category2.get('url', '')
                    main_url_name = category2.get('title', '')
                    if main_url:
                        full_main_url = f"{self.base_url}{main_url}"
                        
                        children2 = category2.get('children', [])
                        for category3 in children2:
                            children3 = category3.get('children', [])
                            for category4 in children3:
                                sub_url = category4.get('url', '')
                                sub_url_name = category4.get('name','')
                                sub_id = category4.get('id','')
                                
                                if sub_url:
                                    full_sub_url = f"{self.base_url}{sub_url}"
                                    response = requests.get(full_sub_url,headers=parser_header)
                                    selector = Selector(response.text)
                                    product_count = selector.xpath("//p[@data-testid = 'page-info-content']/text()").extract_first()
                                    logging.info(f"Product count for {sub_url_name}: {product_count}")
                                    
                                    self.save_to_database(full_main_url, main_url_name, full_sub_url,sub_url_name,sub_id,product_count)

    def save_to_database(self, main_url, category_name, sub_url,sub_url_name,sub_id,product_count):
        try:
            doc = {
                'category_url': main_url,
                'category_name': category_name,
                'sub_category_url': sub_url,
                'sub_category_name': sub_url_name,
                'sub_category_id': sub_id,
                'sub_category_count': product_count
            }
            self.collection_category.insert_one(doc)
            logging.info(f"Saved document to MongoDB: {doc}")
        except Exception as e:
            logging.error(f"Error saving to database: {e}")

    def run(self):
        menu_data = self.fetch_menu()
        self.crawl_data(menu_data)
        logging.info("Data inserted into MongoDB successfully.")

if __name__ == "__main__":
    scraper = CarrefourCategory()
    scraper.run()
