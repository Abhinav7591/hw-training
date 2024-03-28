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
        html_content = self.fetch_page(self.start_url)
        if html_content:
            selector = Selector(html_content)
            yield from self.parse(selector)

    def parse(self, selector):
        rooms = selector.xpath('//article[contains(@class, "ca2f5674")]')
        for room in rooms:
            next_page_behind = room.xpath('.//a/@href').get()
            next_page_url = self.base_url + next_page_behind
            yield next_page_url
            self.scraped_count += 1

        next_page = selector.xpath(
            '//a[@class="b7880daf" and @title="Next"]/@href').get()
        if next_page:
            next_page_url = self.base_url + next_page
            yield from self.parse_next_page(next_page_url)

    def parse_next_page(self, url):
        html_content = self.fetch_page(url)
        if html_content:
            selector = Selector(html_content)
            yield from self.parse(selector)

if __name__ == "__main__":
    collection.create_index([('url', 1)], unique=True)

    start_url = "https://www.bayut.com/to-rent/property/dubai/"
    crawler = BayutCrawler(start_url)

    for room_url in crawler.crawl():
        logger.info(f"Room URL: {room_url}")
        try:
            collection.insert_one({"url": room_url})
            logger.info(f"Inserted Room URL into MongoDB: {room_url}")
            logging_collection.insert_one({"status": "success", "url": room_url})
     

        except DuplicateKeyError:
            logger.warning(f"Room URL already exists in MongoDB: {room_url}")
            logging_collection.insert_one({"status": "duplicate", "url": room_url})
     

        except Exception as e:
            logger.error(f"Error inserting Room URL into MongoDB: {e}")
            logging_collection.insert_one({"status": "failure", "url": room_url})

    logger.info(
        f"Scraped {crawler.scraped_count} Room URLs")

