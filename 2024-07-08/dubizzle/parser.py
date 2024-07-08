from settings import headers
from pipeline import MongoPipeline
from parsel import Selector
from pymongo import errors
from datetime import datetime
import requests
import logging
import time


class DubizzleParser:
    def __init__(self):
        self.mongo_pipeline = MongoPipeline()
        self.collection = self.mongo_pipeline.db["dubizzle_property_urls"]
        self.parsed_collection = self.mongo_pipeline.db['dubizzle_property_data']
        self.error_collection = self.mongo_pipeline.db["dubizzle_parser_error_urls"]

        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    def parse_property_page(self, url):
        timeout = (5, 10)
        try:
            response = requests.get(url, headers=headers,timeout=timeout)           
            if response.status_code == 200:
                selector = Selector(response.text)
                
                property_name = selector.xpath("//h2[@data-testid ='listing-title']/text()").extract_first()
                floor_size = selector.xpath("//p[@data-testid='sqft']/text()").extract_first()
                unit_type = selector.xpath("//div[@data-testid='Type']/text()").extract_first()
                purpose = selector.xpath("//div[@data-testid='Purpose']/text()").extract_first()
                location = selector.xpath("//p[@data-testid ='location-information']/text()").extract_first() or ''
                baths = selector.xpath("//p[@data-testid='bath']/text()").extract_first() or ''
                beds = selector.xpath("//p[@data-testid='bed_space']/text()").extract_first() or ''
                completion = selector.xpath("//div[@data-testid='Completion Status']/text()").extract_first() or ''
                
                
                poster_year_date = selector.xpath("//span[@data-testid='posted-on']/text()").extract()
                filtered_poster_year = [item for item in poster_year_date if item.strip()]
                if filtered_poster_year:
                    relevant_date = filtered_poster_year[0]
                else:
                    relevant_date = ''
                if relevant_date:
                    day_part = ''.join(filter(str.isdigit, relevant_date.split()[0]))
                    month_part = relevant_date.split()[1]
                    year_part = relevant_date.split()[2]

                    clean_date_string = f"{day_part} {month_part} {year_part}"

                    date_obj = datetime.strptime(clean_date_string, "%d %B %Y")
                    poster_year = date_obj.strftime("%Y-%m-%d")
                else:
                    poster_year = ''
                           

                item = {}
                item['url'] = url
                item['property_name'] = property_name
                item['floor_size'] = floor_size
                item['unit_type'] = unit_type
                item['purpose'] = purpose
                item['type'] = purpose
                item['location'] = location
                item['baths'] = baths
                item['beds'] = beds
                item['completion'] = completion
                item['poster_year'] = poster_year
                
        
                logging.info(item)
                try:
                    self.parsed_collection.insert_one(item)
                    logging.info("Item information saved successfully!")
                except Exception as e:
                    logging.error(f"Failed to save item information: {str(e)}")

            elif response.status_code == 404:
                try:
                    self.error_collection.insert_one({'failed_url': url, 'status_code': 404})
                    logging.info(f"Inserted Error URL: {url}")
                except errors.DuplicateKeyError:
                    logging.info(f"Error URL already exists: {url}")
            else:
                logging.error(f"Failed to fetch URL: {url}. Status code: {response.status_code}")

        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed: {str(e)}")
    

    def parse_urls(self, fixed_delay=5):
        for item in self.collection.find():
            url = item.get('property_url', '')
            if url:
                data_url = self.parsed_collection.find_one({'url': url})
                if not data_url:
                    self.parse_property_page(url)
                    time.sleep(fixed_delay)
                
if __name__ == "__main__":
    parser = DubizzleParser()
    parser.parse_urls()