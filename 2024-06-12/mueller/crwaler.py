import requests
import xml.etree.ElementTree as ET
import logging
from pipeline import MongoPipeline
from settings import url

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class MuellerCrawler:
    def __init__(self, sitemap_urls):
        self.sitemap_urls = sitemap_urls
        self.mongo_pipeline = MongoPipeline()

    def fetch_and_save_urls(self):
        collection = self.mongo_pipeline.db["product_urls"]
        
        for sitemap_url in self.sitemap_urls:
            response = requests.get(sitemap_url)
            if response.status_code == 200:
                root = ET.fromstring(response.content)
                urls = [url.text for url in root.iter('{http://www.sitemaps.org/schemas/sitemap/0.9}loc')]
                for url in urls:
                    try:
                        collection.insert_one({'url': url})
                        logging.info(f"Inserted URL into MongoDB: {url}")
                    except Exception as e:
                        logging.error(f"Failed to insert URL {url}: {e}")
            else:
                logging.error(f"Failed to retrieve the sitemap from {sitemap_url}. Status code: {response.status_code}")

if __name__ == "__main__":
    
    sitemap_urls = url  
    sitemap_fetch = MuellerCrawler(sitemap_urls)
    sitemap_fetch.fetch_and_save_urls()
