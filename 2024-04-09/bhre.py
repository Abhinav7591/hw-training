import requests
import logging
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BHGREAgentScraper:
    def __init__(self):
        self.base_url = 'https://www.bhgre.com/'
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        }
        self.mongo_client = MongoClient('mongodb://localhost:27017/')
        self.db = self.mongo_client['bhgre_agents']
        self.collection = self.db['agents']
        self.collection.create_index([('agent_url', 1)], unique=True)

    def fetch_json_data(self, url):
        response = requests.get(url, headers=self.headers)
        if response.status_code == 200:
            return response.json()
        else:
            logger.error(f"Failed to fetch data from {url}: {response.status_code}")
            return None

    def extract_agent_details(self, agent_details_data):
        if agent_details_data is None or 'pageProps' not in agent_details_data or 'detail' not in agent_details_data['pageProps'] or 'agentDetails' not in agent_details_data['pageProps']['detail']:
            logger.warning("Agent details data is not in the expected format.")
            return None

        agent_details = agent_details_data['pageProps']['detail']['agentDetails']
        if 'physicalAddress' not in agent_details:
            logger.warning("Physical address details not found for agent.")
            return None

        image_url = None
        if 'media' in agent_details and agent_details['media'] is not None and 'cdnURL' in agent_details['media']:
            image_url = agent_details['media']['cdnURL']

        return {
            'agent_url': f"{self.base_url}{agent_details['canonicalUrl']}",
            'first_name': agent_details['firstName'],         
            'last_name': agent_details['lastName'],
            'title': agent_details['agentLicenseType'],
            'city': agent_details['physicalAddress']['city'],
            'state': agent_details['physicalAddress']['state'],        
            'country': agent_details['physicalAddress']['country'],
            'zipcode': agent_details['physicalAddress']['zipcode'],
            'email': agent_details['emailAccount'],
            'description': agent_details['profile'],
            'image_url' : image_url,
            'business_phone': agent_details['bussinessPhoneNumber'],
            'cell_phone': agent_details['cellPhoneNumber'],
            'office_website': agent_details['officeWebsite'],
            'language': agent_details['languages'][0]['longDescription'] if agent_details.get('languages') else "Not specified",
        }


    def save_agent_details_to_db(self, agent_details):
        agent_details['agent_url']
        try: 
            self.collection.insert_one(agent_details)
            logger.info(f"Agent details saved to the database: {agent_details['agent_url']}")

        except DuplicateKeyError:     
            logger.info(f"Agent details already exist for {agent_details['agent_url']}. Ignoring...")
            pass 


    def scrape_agents(self):
        first_page_url = self.base_url + '_next/data/3XujSuQEX7YqiySOqIcRq/sitemap/agents.json'
        data = self.fetch_json_data(first_page_url)

        if data and 'pageProps' in data and 'sitemapListContent' in data['pageProps']:
            urls = [link['url'] for link in data['pageProps']['sitemapListContent']['siteMapLinks']]
            for url in urls:
                json_url = f"{self.base_url}_next/data/3XujSuQEX7YqiySOqIcRq/{url}.json"
                sub_urls_data = self.fetch_json_data(json_url)

                if sub_urls_data:
                    sub_urls = [link['url'] for link in sub_urls_data['pageProps']['sitemapTopCitiesListContent']['siteMapLinks']]
                    logger.info(f"Scraping Sub URLs for {url}")
                    for sub_url in sub_urls:
                        logger.info(f"Scraping {sub_url}")
                        page_number = 1
                        while True:
                            agent_url = f"{self.base_url}_next/data/3XujSuQEX7YqiySOqIcRq/{sub_url}.json?page={page_number}"
                            agent_data = self.fetch_json_data(agent_url)
                            if agent_data and 'pageProps' in agent_data and 'results' in agent_data['pageProps'] and 'agents' in agent_data['pageProps']['results']:
                                agents = agent_data['pageProps']['results']['agents']
                                for agent in agents:
                                    if 'canonicalUrl' in agent:
                                        agent_canonical_url = agent['canonicalUrl']
                                        logger.info(f"Scraping agent details from {agent_canonical_url}")
                                        agent_details_url = f"{self.base_url}_next/data/3XujSuQEX7YqiySOqIcRq/{agent_canonical_url}.json"
                                        agent_details_data = self.fetch_json_data(agent_details_url)
                                        if agent_details_data:
                                            agent_details = self.extract_agent_details(agent_details_data)
                                            self.save_agent_details_to_db(agent_details)
                                page_number += 1
                            else:
                                logger.warning(f"No agents found for {sub_url} on page {page_number}")
                                break
                else:
                    logger.warning(f"No sub URLs found for {url}")
        else:
            logger.error("The structure of the JSON response is not as expected.")

def main():
    scraper = BHGREAgentScraper()
    scraper.scrape_agents()

if __name__ == "__main__":
    main()
