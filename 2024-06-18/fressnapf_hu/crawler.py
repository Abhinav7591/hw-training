import requests
import re
from pymongo import errors
from settings import sitemap_urls
from pipeline import MongoPipeline

class SitemapScraper:
    def __init__(self, sitemap_urls):
        self.sitemap_urls = sitemap_urls
        self.mongo_pipeline = MongoPipeline()
        self.collection = self.mongo_pipeline.db["product_urls"]

    def fetch_sitemap_urls(self, sitemap_url):
        response = requests.get(sitemap_url)
        if response.status_code != 200:
            print(f"Failed to fetch the sitemap. Status code: {response.status_code}")
            return []

        urls = re.findall(r"<loc>(.*?)</loc>", response.text)
        return urls

    def scrape(self):       
        for sitemap_url in self.sitemap_urls:
            urls = self.fetch_sitemap_urls(sitemap_url)
            for url in urls:
                try:
                    self.collection.insert_one({"url": url})
                    print(f"Inserted URL: {url}")
                except errors.DuplicateKeyError:
                    print(f"Duplicate URL skipped: {url}")

if __name__ == "__main__":
    sitemap_urls = sitemap_urls

    scraper = SitemapScraper(sitemap_urls)
    scraper.scrape()
