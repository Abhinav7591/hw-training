from settings import headers_product, base_url
from pipeline import MongoPipeline
from pymongo import errors
import requests
import logging

class CarrefourCrawler:
    def __init__(self):
        self.base_url = base_url
        self.headers_product = headers_product
        self.mongo_pipeline = MongoPipeline()
        self.collection = self.mongo_pipeline.db["carrefouruae_product_urls"]
        self.error_collection = self.mongo_pipeline.db["carrefouruae_crawler_error_urls"]
        self.category_collection = self.mongo_pipeline.db["carrefouruae_category_urls"]
        
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        
    def fetch_products(self, category_id, sub_category_url):
        headers = self.headers_product
        page = 0
        product_data = []
        
        while True:
            url = f'https://www.carrefouruae.com/api/v8/categories/{category_id}?filter=&sortBy=relevance&currentPage={page}&pageSize=60&maxPrice=&minPrice=&areaCode=DubaiFestivalCity-Dubai&lang=en&displayCurr=AED&latitude=25.2171003&longitude=55.3613635&needVariantsData=true&nextOffset=&needVariantsData=true&requireSponsProducts=true&responseWithCatTree=true&depth=3'
            
            response = requests.get(url, headers=headers)
            if response.status_code == 404:
                try:
                    self.error_collection.insert_one({'url': url, 'status_code': 404})
                    logging.info(f"Inserted Error URL: {url}")
                except errors.DuplicateKeyError:
                    logging.warning(f"Error URL already exists: {url}")
                break
            
            products_data = response.json()

            products = products_data.get('products', [])
            if not products:
                break

            for product in products:
                product_url = product.get('links', {}).get('productUrl', {}).get('href', '')
                if product_url:
                    product_data.append({'url': f'{base_url}{product_url}', 'sub_url': sub_category_url})

            page += 1
        return product_data

    def fetch_all_category(self):
        
        all_ids = self.category_collection.distinct("sub_category_id")
        sub_urls = self.category_collection.distinct("sub_category_url")
        for category_id, sub_category_url in zip(all_ids, sub_urls):
            product_data = self.fetch_products(category_id, sub_category_url)
            if product_data:
                for data in product_data:
                    try:
                        self.collection.insert_one(data)
                        logging.info(f"Inserted Product Data: {data}")
                    except errors.DuplicateKeyError:
                        logging.warning(f"Product Data already exists: {data}")

if __name__ == "__main__":
    scraper = CarrefourCrawler()
    scraper.fetch_all_category()
