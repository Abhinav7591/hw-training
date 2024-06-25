from settings import category_headers,headers_product_urls,url_product_urls,url_search
from pipeline import MongoPipeline
import requests
from pymongo import errors


class MigrosProductScraper:
    def __init__(self):
        self.mongo_pipeline = MongoPipeline()
        self.collection = self.mongo_pipeline.db["product_urls"]
        self.headers_search = category_headers
        self.headers_product_urls = headers_product_urls
        
        self.url_search = url_search
        self.url_product_urls = url_product_urls

    def fetch_product_ids(self, category_id):
        payload = {
            "regionId": "national",
            "language": "de",
            "categoryId": category_id,
            "productIds": [],
            "sortFields": [],
            "sortOrder": "asc",
            "from": 0,
            "algorithm": "DEFAULT",
            "filters": {},
        }

        response = requests.post(self.url_search, headers=self.headers_search, json=payload)
        if response.status_code == 200:
            response_data = response.json()
            return response_data.get('productIds', [])
        else:
            print(f"Failed to fetch product IDs for Category ID {category_id}. Status code: {response.status_code}")
            return []

    def fetch_product_urls(self, product_ids):
        payload = {
            "offerFilter": {
                "storeType": "ONLINE",
                "warehouseId": 1,
                "ongoingOfferDate": "2024-06-24T00:00:00"
            },
            "productFilter": {
                "uids": product_ids
            },
        }

        response = requests.post(self.url_product_urls, headers=self.headers_product_urls, json=payload)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Failed to fetch product URLs. Status code: {response.status_code}")
            return []

    def store_urls_in_mongodb(self, urls):
        for item in urls:
            product_url = item.get('productUrls', '')
            if product_url:
                try:
                    self.collection.insert_one({'url': product_url})
                    print(f"Inserted URL: {product_url}")
                except errors.DuplicateKeyError:
                    print(f"URL already exists: {product_url}")

    def scrape(self, start_category_id, end_category_id):
        for category_id in range(start_category_id, end_category_id + 1):
            print(f" Category ID: {category_id}")
            product_ids = self.fetch_product_ids(category_id)
            urls_data = self.fetch_product_urls(product_ids)
            self.store_urls_in_mongodb(urls_data)

if __name__ == "__main__":
    scraper = MigrosProductScraper()
    scraper.scrape(start_category_id=7494730, end_category_id=7494900)
