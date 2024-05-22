import requests
from parsel import Selector
from pymongo import MongoClient
import logging
import re

class LongandfoasterParser:
    def __init__(self):
        self.mongo_client = MongoClient('mongodb://localhost:27017/')
        self.db = self.mongo_client['longandfoster']
        self.collection = self.db['agent_urls']
        self.parsed_collection = self.db['agent_details']
        self.parsed_collection.create_index([('profile_url', 1)], unique=True)

        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    def parse_agent_page(self, url):
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3",
            "Authority": "www.longandfoster.com",
            "Referer": url,
            "Sec-Ch-Ua": '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Linux"',
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "X-Requested-With": "XMLHttpRequest"
        }
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            selector = Selector(response.text)

            name = selector.xpath("//h1[@class ='lf_header--primary lf_header--no-margin-top lf_visible-xs lf_col-xxs-12']/text()").get()
            if name:
                name = name.strip().split(" ")
                first_name = name[0]
                middle_name = name[1] if len(name) > 1 else ""
                last_name = name[2] if len(name) > 2 else ""
            else:
                first_name = middle_name = last_name = ""

            about_data = selector.xpath("//div[@id ='agent-biography']//text()").getall()
            description = ' '.join(about_data).strip() if about_data else "N/A"

            languages = selector.xpath("//div[h3[text()='Foreign Languages']]/following-sibling::div/text()").get() or 'N/A'
            image_url = selector.xpath("//figure[@class ='lf_agent-profile__photo lf_photo lf_bordered lf_bordered--dark']/img/@src").get()

            main_address = selector.xpath("//div[@class='lf_agent-profile__information__cell lf_table__cell']/br[1]/following-sibling::text()[1]").get().strip()

            address = selector.xpath("//div[@class='lf_agent-profile__information__cell lf_table__cell']/br[2]/following-sibling::text()[1]").get()
            address =address.strip().split(",")
            city = address[0]
            state_zip =address[1].split(" ")
            state = state_zip[1]
            zipcode = state_zip[2]

            office_phone_numbers_text = selector.xpath("//div[@class='ai_phone_directoffice']/text()").get()
            office_phone_numbers = ''.join(filter(str.isdigit, office_phone_numbers_text)) if office_phone_numbers_text else "N/A"

            agent_phone_numbers_text = selector.xpath("//div[@class='ai_phone_mobile']/text()").get()
            agent_phone_numbers = ''.join(filter(str.isdigit, agent_phone_numbers_text)) if agent_phone_numbers_text else "N/A"

            website = selector.xpath("//div[h3[text()='Website']]/following-sibling::div/a/@href").get()
            if website is not None:
                website = re.search(r"open_AgentWebsite\('(.*?)'\)", website)
                website = website.group(1) if website else None

            social = selector.xpath("//ul[@class ='list-inline lf_social']/li/a/@href").getall() or 'N/A'

            agentcd_script = selector.xpath("//script[contains(text(), 'getAgentData')]/text()").get()
            agent_cd_match = re.search(r'agentCd\s*=\s*"(\d+)";', agentcd_script)
            agent_cd = agent_cd_match.group(1) if agent_cd_match else None
            if agent_cd:
                api_url = f"https://www.longandfoster.com/include/ajax/api.aspx?op=LNFAPI&type=AgentData&parameters=selectStr%3DEmail%26query%3DAgentNumber%253D%2522{agent_cd}%2522"
                email_response = requests.get(api_url, headers=headers)

                if email_response.status_code == 200:
                    email_data = email_response.json()
                    email = email_data['Results'][0]['Email'] if 'Results' in email_data and email_data['Results'] else None
                else:
                    email = None
            else:
                email = None


            agent_info = {
                'profile_url': url,
                'first_name': first_name,                
                'middle_name': middle_name,
                'last_name' : last_name,
                'description' : description,
                'languages' : languages,
                'image_url' : image_url,                  
                'address' : main_address,
                'city' : city,
                'state' : state,
                'zipcode' :zipcode,  
                'office_phone_number' : office_phone_numbers,
                'agent_phone_number' : agent_phone_numbers,
                'email' : email,
                'website' : website, 
                'social' : social,   
                     
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

parser = LongandfoasterParser()
parser.parse_urls()