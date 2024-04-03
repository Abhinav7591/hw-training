import logging
import requests
from parsel import Selector
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

client = MongoClient("mongodb://localhost:27017/")
db = client["bayut_datas"]
collection = db["room_urls"]
logging_collection = db["logging_details"]

class BayutCrawler:
    def __init__(self, start_url):
        self.start_url = start_url
        self.base_url = "https://www.bayut.com"
        self.scraped_count = 0

    def fetch_page(self, url):
        response = requests.get(url)
        if response.status_code == 200:
            return response.text
        else:
            logger.error(f"Failed to fetch page {url}")
            return None

    def crawl(self):
        current_url = self.start_url
        while current_url:
            html_content = self.fetch_page(current_url)
            if html_content:
                selector = Selector(html_content)
                yield from self.parse(selector)
                current_url = self.next_page_url(selector)
            else:
                current_url = None

    def parse(self, selector):
        property = selector.xpath('//article[contains(@class, "ca2f5674")]')
        for properties in property:
            next_page_behind = properties.xpath('.//a/@href').get()
            yield self.base_url + next_page_behind
            self.scraped_count += 1

    def next_page_url(self, selector):
        next_page = selector.xpath('//a[@class="b7880daf" and @title="Next"]/@href').get()
        if next_page:
            return self.base_url + next_page
        else:
            return None

if __name__ == "__main__":
    collection.create_index([('url', 1)], unique=True)

    start_url = "https://www.bayut.com/to-rent/property/dubai/"
    crawler = BayutCrawler(start_url)

    for property_url in crawler.crawl():
        logger.info(f"Property URL: {property_url}")
        try:
            collection.insert_one({"url": property_url})
            logger.info(f"Inserted Property URL into MongoDB: {property_url}")
            logging_collection.insert_one({"status": "success", "url": property_url})   

        except DuplicateKeyError:
            logger.warning(f"Property URL already exists in MongoDB: {property_url}")
            logging_collection.insert_one({"status": "duplicate", "url": property_url})
     
        except Exception as e:
            logger.error(f"Error inserting Property URL into MongoDB: {e}")
            logging_collection.insert_one({"status": "failure", "url": property_url})

    logger.info(
        f"Scraped {crawler.scraped_count} Property URLs")
