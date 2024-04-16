import requests
from parsel import Selector
from pymongo import MongoClient
import logging

class SitemapParser:
    def __init__(self):
        self.mongo_client = MongoClient('mongodb://localhost:27017/')
        self.db = self.mongo_client['reecenichols']
        self.collection = self.db['agent_urls']
        self.parsed_collection = self.db['agent_details']
        self.parsed_collection.create_index([('url', 1)], unique=True)

        logging.basicConfig(level=logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s')

    def parse_agent_page(self, url):
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
        }
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            selector = Selector(response.text)

            name = selector.xpath("//article/p[@class ='rng-agent-profile-contact-name']/text()").get().strip()
            profile = selector.xpath("//article/p[@class ='rng-agent-profile-languages']/text()").get()
            Awards_Badges = selector.xpath("//article/p[@class ='rng-agent-profile-languages'][2]/text()").get()
            phone = selector.xpath("//article[@class='rng-agent-profile-main']//li[@class='rng-agent-profile-contact-phone']/a/text()").get()

            about_data = selector.xpath('//article[@class="rng-agent-profile-content"]//div[@id="body-text-1-preview-5500-4008334"]/p/text()').getall()
            about_text = ' '.join(about_data).strip() if about_data else "N/A"


            agent_info = {
                'url': url,
                'name': name,
                'profile': profile,
                'Awards_Badges': Awards_Badges,
                'phone': phone,
                'about': about_text,
            }

            logging.info(agent_info)

            try:
                self.parsed_collection.insert_one(agent_info)
                logging.info("Agent information saved successfully!")
            except Exception as e:
                logging.error(f"Failed to save agent information: {str(e)}")

            else:
                logging.error(f"Failed to fetch URL: {url}. Status code: {response.status_code}")


    def parse_urls(self):
        while self.collection.count_documents({}) > 0:
            agent_doc = self.collection.find_one_and_delete({})
            agent_url = agent_doc["url"]
            self.parse_agent_page(agent_url)

parser = SitemapParser()
parser.parse_urls()

