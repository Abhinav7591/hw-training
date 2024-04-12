import requests
from parsel import Selector
from pymongo import MongoClient
import time
import logging

class SitemapParser:
    def __init__(self):
        self.mongo_client = MongoClient('mongodb://localhost:27017/')
        self.db = self.mongo_client['bhgre_sitemap']
        self.collection = self.db['agent_urls']
        self.parsed_collection = self.db['agent_details']
        self.parsed_collection.create_index([('url', 1)], unique=True)

        logging.basicConfig(filename='parser.log',level=logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s')

    
    def parse_agent_page(self, url):
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
        }
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            selector = Selector(response.text)

            full_name = selector.xpath("//div[contains(@class, 'MuiBox-root')]/h1[contains(@class, 'MuiTypography-root')]/text()").get()
            title = selector.xpath("//div[contains(@class, 'MuiBox-root css-kxp5fs')]/p[contains(@class, 'MuiTypography-root MuiTypography-body2 css-60lj4b-MuiTypography-root')]/text()").get()

            location_text = selector.xpath("//div[contains(@class, 'MuiBox-root css-kxp5fs')]/span[contains(@class, 'MuiTypography-root MuiTypography-body3 css-1ud1z3l-MuiTypography-root')]/text()").get()
            city_state = location_text.split(', ') if location_text else [None, None]

            office_name = selector.xpath("//div[@id='agentOffice']/h6[@id='agentName']/text()").get()
        
            phone_number = selector.xpath("//a[contains(@class, 'css-o4a0jf')]/span[@class='css-q896e1']/text()").get()
            email = selector.xpath("//a[contains(@href, 'mailto')]/span/text()").get()

            description = selector.xpath("//p[contains(@class, 'AgentProfile_clipText')]/text()").getall()
            description = " ".join(description).strip() if description else None

            language = selector.xpath("//h6[@id='language']/following-sibling::div/p/text()").get()

            social_link = selector.xpath("//div[@class='MuiBox-root css-y1gt6f']//div[@class ='MuiStack-root css-w4z10b-MuiStack-root']/a/@href").get()


            agent_info = {
                'url': url,
                'full_name': full_name,
                'title': title,
                'city': city_state[0],
                'state': city_state[1],
                'office': office_name,
                'phone_number': phone_number,
                'email' : email,
                'description': description,
                'language': language,
                'social' : social_link,
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
            agent_url = agent_doc["agent_url"]
            self.parse_agent_page(agent_url)
            time.sleep(2)


parser = SitemapParser()
parser.parse_urls()

