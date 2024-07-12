from settings import headers
from pipeline import MongoPipeline
from parsel import Selector
from pymongo import errors
import requests
import logging
import time
import json
import re


class PropertyFinderParser:
    def __init__(self):
        self.mongo_pipeline = MongoPipeline()
        self.collection = self.mongo_pipeline.db["Propertyfinder_property_url"]
        self.parsed_collection = self.mongo_pipeline.db['Propertyfinder_property_data']
        self.error_collection = self.mongo_pipeline.db["Propertyfinder_parser_error_urls"]

        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    def parse_property_page(self, url):
        timeout = (5, 10)
        try:
            response = requests.get(url, headers=headers,timeout=timeout)           
            if response.status_code == 200:
                selector = Selector(response.text)
                
                product_name_xpath = "//h1[@class ='styles_desktop_title__QuRdg']/text()"
                agent_name_xpath = "//p[@class ='styles_desktop_agent__name__jYT_I styles_desktop_agent__name--highlight__pRQkM']/text()"
                location_xpath = "//p[@class ='styles_desktop_subtitle__ZxS1V']/text()"
                unit_type_xpath = "//div[@class='styles_desktop_list__item__lF_Fh']//p[@class='styles_desktop_list__value__uIdMl']//text()"
                description_xpath = "//div[@data-testid='collapsing-description']//article//text()"
                price_data_xpath = "//p[@class ='styles_desktop_price__text__Eb_Ti styles_attributes__price__7fU1B']/text()"
                amenities_xpath = "//div[@class='styles_amenity__c2P5u']//p"
                locality_xpath = "//p[@class='styles_desktop_full-name__44zGs']//text()"
                attribute_xpath = "//div[@class='styles_attributes__list-item__q1sPg']//text()"
                availability_xpath = "//div[@class='styles_desktop_list__item__lF_Fh']//p[@class='styles_desktop_list__value__uIdMl']//text()"
                script_xpath = "//script[@id='__NEXT_DATA__']/text()"
                
                category = 'Sale' if 'sale' in url else 'Rent' if 'rent' in url else ''
                itemid = re.search(r'(\d\d\d+)', url).group(1)
                
                product_name = selector.xpath(product_name_xpath).extract_first()
                agent_name = selector.xpath(agent_name_xpath).extract_first()
                location = selector.xpath(location_xpath).extract_first()     
                unit_type = selector.xpath(unit_type_xpath).extract_first()   
                
                description = selector.xpath(description_xpath).extract()
                property_description = ''.join(description)
                
                price_data = selector.xpath(price_data_xpath).extract_first()   
                if price_data:
                    price = price_data.split()[0].replace(',', '') 
                    currency = price_data.split()[1] 
                else:
                    price = '' 
                    currency =''               
                
                
                amenities_data = selector.xpath(amenities_xpath)
                amenity_data = []
                for items in amenities_data:
                    amenity = items.xpath(".//text()").extract_first()
                    amenity_data.append(amenity)
                amenities = ", ".join(amenity_data)
                
                
                locality_data = selector.xpath(locality_xpath).extract_first()
                if locality_data:
                    locality_match = re.search(r'^[^,]+', locality_data)
                    if locality_match:
                        adress_locality = locality_match.group(0).strip()
                    else:
                        adress_locality = ''
                else:
                    adress_locality = ''
                    
                    
                attribute_list = selector.xpath(attribute_xpath).extract()
                attribute_cleaned = [item.strip() for item in attribute_list if item.strip()]
                
                bedrooms = ''
                bathrooms = ''
                property_size = ''
                
                for i, item in enumerate(attribute_cleaned):
                    if 'Bedrooms' in item:
                        bedrooms = attribute_cleaned[i-1] 
                    if 'Bathrooms' in item:
                        bathrooms = attribute_cleaned[i-1] 
                if attribute_cleaned:
                    property_size = attribute_cleaned[-1]
                            
                            
                availablity = selector.xpath(availability_xpath).extract()
                avail_pattern = re.compile(r'\b\d{1,2} \w{3} \d{4}\b')
                availability_date = next((item for item in availablity if avail_pattern.match(item)), '')
                          
                                
                script_data = selector.xpath(script_xpath).get()   
                if script_data:
                    json_data = json.loads(script_data)
                
                    property_location = json_data.get('props', {}).get('pageProps', {}).get('propertyResult', {}).get('property', {}).get('location', {}).get('coordinates', '')
                    if isinstance(property_location, dict):
                        latitude = property_location.get('lat', '')
                        longitude = property_location.get('lon', '')
                        geolocation = f"{latitude},{longitude}"
                    else:
                        geolocation =''
                
                    property_data = json_data.get('props', {}).get('pageProps', {}).get('propertyResult', {}).get('property', {}).get('offering_type', '')
                    property_type = property_data.split()[0] if property_data else ''
                  

                item = {}
                item['url'] = url
                item['product_name'] = product_name
                item['agent_name'] = agent_name
                item['location'] = location
                item['price'] = price
                item['currency'] = currency
                item['unit_type'] = unit_type
                item['geolocation'] = geolocation
                item['property_description'] = property_description
                item['amenities'] = amenities
                item['address_locality'] = adress_locality
                item['category'] = category
                item['type'] = category
                item['property_name'] = product_name
                item['itemid'] = itemid
                item['bedrooms'] = bedrooms
                item['bathrooms'] = bathrooms
                item['property_size'] = property_size
                item['availability_date'] = availability_date
                item['address_region'] = location
                item['property_type'] = property_type
        
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
    parser = PropertyFinderParser()
    parser.parse_urls()  