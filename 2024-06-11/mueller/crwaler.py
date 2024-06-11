import requests
import xml.etree.ElementTree as ET
import logging
from pipeline import get_mongo_collection
from settings import url

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class MuellerCrawler:
    def __init__(self, sitemap_urls, collection):
        self.sitemap_urls = sitemap_urls
        self.collection = collection

    def fetch_urls(self, sitemap_url):
        response = requests.get(sitemap_url)
        if response.status_code == 200:
            root = ET.fromstring(response.content)
            urls = [url.text for url in root.iter('{http://www.sitemaps.org/schemas/sitemap/0.9}loc')]
            return urls
        else:
            logging.error(f"Failed to retrieve the sitemap from {sitemap_url}. Status code: {response.status_code}")
            return []

    def get_all_urls(self):
        all_urls = []
        for sitemap_url in self.sitemap_urls:
            all_urls.extend(self.fetch_urls(sitemap_url))
        return all_urls

    def save_to_mongodb(self, urls):
        for url in urls:
            try:
                self.collection.insert_one({'url': url})
                logging.info(f"Inserted URL into MongoDB: {url}")
            except Exception as e:
                logging.error(f"Failed to insert URL {url}: {e}")

    def fetch_and_save_urls(self):
        urls = self.get_all_urls()
        self.save_to_mongodb(urls)

if __name__ == "__main__":
    sitemap_urls = url()  
    collection = get_mongo_collection('mueller', 'product_urls') 

    sitemap_fetcher = MuellerCrawler(sitemap_urls, collection)
    sitemap_fetcher.fetch_and_save_urls()
