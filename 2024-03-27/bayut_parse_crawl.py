import re
import requests
from parsel import Selector
import logging
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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

        next_page = selector.xpath('//a[@class="b7880daf" and @title="Next"]/@href').get()
        if next_page:
            next_page_url = self.base_url + next_page
            yield from self.parse_next_page(next_page_url)

    def parse_next_page(self, url):
        html_content = self.fetch_page(url)
        if html_content:
            selector = Selector(html_content)
            yield from self.parse(selector)

class BayutParser:
    def __init__(self):
        pass

    def parse_room_page(self, selector):
 
        property_id = selector.xpath("//li[span[contains(text() ,'Reference no.')]]/span[2]/text()").get()
        purpose = selector.xpath("//li[span[contains(text() ,'Purpose')]]/span[2]/text()").get()
        type = selector.xpath("//li[span[contains(text() ,'Type')]]/span[2]/text()").get()
        added_on = selector.xpath("//li[span[contains(text() ,'Added on')]]/span[2]/text()").get()
        furnishing = selector.xpath("//li[span[contains(text() ,'Furnishing')]]/span[2]/text()").get()
        price_currency = selector.xpath("//div[@aria-label='Property basic info']//span[@aria-label='Currency']/text()").get()
        price_amount = selector.xpath("//div[@aria-label='Property basic info']//span[@aria-label='Price']/text()").get()
        location = selector.xpath("//div[@aria-label='Property header']/text()").get()

        beds_text = selector.xpath("//div[@aria-label='Property basic info']//span[@aria-label='Beds']/span/text()").get()
        beds = int(re.findall('\d+', beds_text)[0]) if beds_text and re.findall(r'\d+', beds_text) else None

        baths_text = selector.xpath("//div[@aria-label='Property basic info']//span[@aria-label='Baths']/span/text()").get()
        baths = int(re.findall('\d+', baths_text)[0]) if baths_text and re.findall(r'\d+', baths_text) else None

        area = selector.xpath("//div[@aria-label='Property basic info']//span[@aria-label='Area']//span/text()").get()

        nav_items = selector.xpath('//div[@class="_8468d109"]//a//text()').getall()
        breadcrumbs = " > ".join(nav_items)
        
        amenities= selector.xpath("//div[@aria-label='Dialog']//span/text()").getall()
        agent_name = selector.xpath("//a[@aria-label='Agent name']/text()").get()
        image_url = selector.xpath("//picture/img[@aria-label='Cover Photo']/@src").get()
        description_list = selector.xpath("//div[@aria-label='Property description']//span/text()").getall()
        description = ' '.join(description_list)

        room_data = {
            'property_id': property_id,
            'purpose': purpose,
            'type': type,
            'added_on': added_on,
            'furnishing': furnishing,
            'price': {
                'currency': price_currency,
                'amount': price_amount,
            },
            'location': location,
            'bed_bath_size': {
                'bedrooms': beds,
                'bathrooms': baths,
                'size': area,
            },
            'breadcrumbs': breadcrumbs,
            'amenities': amenities,
            'agent_name': agent_name,
            'image_url': image_url,
            'description': description,
        }
        return room_data


if __name__ == "__main__":

    client = MongoClient("mongodb://localhost:27017/")
    db = client[ "bayut_datas"]
    collection = db["room_urls"]

    collection.create_index([('url', 1)], unique=True)

    start_url = "https://www.bayut.com/to-rent/property/dubai/"
    crawler = BayutCrawler(start_url)

    for room_url in crawler.crawl():
        logger.info(f"Room URL: {room_url}")
        try:
            collection.insert_one({"url": room_url})
            logger.info(f"Inserted room URL into MongoDB: {room_url}")
        except DuplicateKeyError:
            logger.warning(f"Room URL already exists in MongoDB: {room_url}")
        except Exception as e:
            logger.error(f"Error inserting room URL into MongoDB: {e}")

    logger.info(f"Scraped {crawler.scraped_count} room URLs")
