import requests
import xml.etree.ElementTree as ET
from pymongo import MongoClient
import logging

class SitemapCrawler:
    def __init__(self, sitemap_url):
        self.sitemap_url = sitemap_url
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
        }
        self.mongo_client = MongoClient('mongodb://localhost:27017/')
        self.db = self.mongo_client['bhgre_sitemap']
        self.collection = self.db['agent_urls']
        self.collection.create_index([('agent_url', 1)], unique=True)
        
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

 
    def extract_agent_urls(self, sitemap_url):
        response = requests.get(sitemap_url, headers=self.headers)
        
        if response.status_code == 200:
            root = ET.fromstring(response.content)

            agent_urls = []

            for url in root.findall('{http://www.sitemaps.org/schemas/sitemap/0.9}url'):
                loc = url.find('{http://www.sitemaps.org/schemas/sitemap/0.9}loc').text
                agent_urls.append(loc)
            
            return agent_urls
        else:
            logging.error(f"Failed to fetch sitemap: {sitemap_url}. Status code: {response.status_code}")
            return None


    def extract_states_urls(self):
        response = requests.get(self.sitemap_url, headers=self.headers)

        if response.status_code == 200:
            root = ET.fromstring(response.content)
            
            for sitemap in root.findall('{http://www.sitemaps.org/schemas/sitemap/0.9}sitemap'):
                loc = sitemap.find('{http://www.sitemaps.org/schemas/sitemap/0.9}loc').text
                
                agent_urls = self.extract_agent_urls(loc)
                if agent_urls:
                    logging.info(f"Agent URLs for state {loc}:")
                    for agent_url in agent_urls:
                        logging.info(agent_url)
                        if not self.collection.find_one({'agent_url': agent_url}):
                            self.collection.insert_one({'agent_url': agent_url})  
                        else:
                            logging.warning(f"Duplicate agent URL found: {agent_url}")
        else:
            logging.error(f"Failed to fetch sitemap. Status code: {response.status_code}")
        

sitemap_extractor = SitemapCrawler("https://www.bhgre.com/xml-sitemap/states/sitemapindex-agents.xml")
sitemap_extractor.extract_states_urls()
