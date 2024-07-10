from settings import headers,cookies
from pipeline import MongoPipeline
from parsel import Selector
from pymongo import errors
from datetime import datetime
import requests
import logging
import time
import json
import re


class DubizzleParser:
    def __init__(self):
        self.mongo_pipeline = MongoPipeline()
        self.collection = self.mongo_pipeline.db["dubizzle_property_url"]
        self.parsed_collection = self.mongo_pipeline.db['dubizzle_property_data']
        self.error_collection = self.mongo_pipeline.db["dubizzle_parser_error_urls"]

        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    def parse_property_page(self, url):
        timeout = (5, 10)
        try:
            response = requests.get(url, headers=headers,timeout=timeout,cookies=cookies)           
            if response.status_code == 200:
                selector = Selector(response.text)
                
                property_name_xpath = "//h2[@data-testid ='listing-title']/text()"
                floor_size_xpath = "//p[@data-testid='sqft']/text()"
                unit_type_xpath = "//div[@data-testid='Type']/text()"
                purpose_xpath = "//div[@data-testid='Purpose']/text()"
                location_xpath = "//p[@data-testid ='location-information']/text()"
                bathroom_xpath = "//p[@data-testid='bath']/text()"
                bedroom_xpath = "//p[@data-testid='bed_space']/text()"
                completion_xpath = "//div[@data-testid='Completion Status']/text()"
                property_age_xpath = "//div[@data-testid='Property Age']/text()"
                furnishing_xpath = "//div[@data-testid='Furnishing']/text()"
                poster_year_xpath = "//span[@data-testid='posted-on']/text()"
                price_xpath = "//div[@data-testid ='listing-price']/p/text()"
                ownership_xpath = "//div[@data-testid ='Ownership']/text()"
                parking_availability_xpath = "//div[@data-testid ='Parking Availability']/text()"
                script_xpath = "//script[@id = '__NEXT_DATA__']/text()"
                
                property_name = selector.xpath(property_name_xpath).extract_first()
                floor_size = selector.xpath(floor_size_xpath).extract_first()
                unit_type = selector.xpath(unit_type_xpath).extract_first()
                purpose = selector.xpath(purpose_xpath).extract_first()
                location = selector.xpath(location_xpath).extract_first() or ''
                completion = selector.xpath(completion_xpath).extract_first() or ''
                property_age = selector.xpath(property_age_xpath).extract_first() or ''                  
                furnishing = selector.xpath(furnishing_xpath).extract_first()  or ''
                
                baths_data = selector.xpath(bathroom_xpath).extract_first()
                if baths_data:
                    baths = re.search(r'\d+', baths_data).group()
                else:
                    baths = ''
                 
                beds_data = selector.xpath(bedroom_xpath).extract_first()
                if beds_data:
                    beds = re.search(r'\d+', beds_data).group()
                else:
                    beds = ''
                                          
                poster_year_date = selector.xpath(poster_year_xpath).extract()
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
                    
                price_data = selector.xpath(price_xpath).extract()
                price = price_data[1] if price_data else ''           
                currency = price_data[0] if price_data else ''  
                

                
                ownership = selector.xpath(ownership_xpath).extract_first()
                parking_availability = selector.xpath(parking_availability_xpath).extract_first()
                if ownership and parking_availability:
                    validated_information = {
                        'Ownership': ownership.strip() if ownership else '',
                        'Parking Availability': parking_availability.strip() if parking_availability else ''
                    }
                else:
                    validated_information = ''
                    
                    
                if '/commercial/' in url:
                    property_type = 'Commercial'
                elif '/residential/' in url:
                    property_type = 'Residential'
                else:
                    property_type = ''
                     
                
                script_data = selector.xpath(script_xpath).extract_first()
                if script_data:
                    script_data = json.loads(script_data)
                    data = script_data.get('props', {}).get('pageProps', {}).get('reduxWrapperActionsGIPP', {})

                    amenities_str = ""
                    history_info = []
                    description = ""
                    property_id = ""
                    geolocation = ""

                    for item in data:
                        payload = item.get('payload', {})
                        
                        if isinstance(payload, dict) and isinstance(payload.get('description'), dict):
                            description = payload['description'].get('en', '')
                        

                        meta = item.get('meta', {})
                        if isinstance(meta, dict) and isinstance(meta.get('arg'), dict):
                            listing_id = meta['arg'].get('listingId', '')
                            if listing_id:
                                property_id = listing_id

                        if isinstance(payload, dict) and isinstance(payload.get('amenities'), list):
                            amenities = payload['amenities']
                            for amenity in amenities:
                                amenity_name_en = amenity.get('name', {}).get('en', '')
                                if amenity_name_en:
                                    if amenities_str:
                                        amenities_str += ", "
                                    amenities_str += amenity_name_en
                        

                        if isinstance(payload, dict) and isinstance(payload.get('coordinates'), dict):
                            latitude = payload['coordinates'].get('lat', '')
                            longitude = payload['coordinates'].get('lng', '')
                            if latitude and longitude:
                                geolocation = f"{latitude},{longitude}"
                                

                        if isinstance(payload, dict) and isinstance(payload.get('rent_transaction_info'), list):
                            rent_histories = payload['rent_transaction_info']
                            for rent_history in rent_histories:
                                rent_date = rent_history.get('date_transaction', '')
                                rent_price = rent_history.get('transaction_amount', '')
                                if rent_date and rent_price:
                                    history_info.append({'Date': rent_date, 'Price': rent_price})
                                    
                        
                        # if isinstance(payload, dict) and isinstance(payload.get('sale_transaction_info'), list):
                        #     histories = payload['sale_transaction_info']
                        #     for history in histories:
                        #         sale_date = history.get('date_transaction', '')
                        #         sale_price = history.get('transaction_amount', '')
                        #         if sale_date and sale_price:
                        #             history_info.append({'Date': sale_date, 'Price': sale_price})
                                    
                                

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
                item['price'] = price
                item['currency'] = currency
                item['property_age'] = property_age
                item['categories'] = purpose
                item['furnishing'] = furnishing
                item['item_id'] = property_id
                item['validated_information'] = validated_information
                item['property_type'] =  property_type
                item['description'] = description
                item['amenities'] = amenities_str
                item['goelocation'] = geolocation
                item['history_info'] = history_info
                
        
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
    
    
    
                    
                # script_data = selector.xpath("//script[@id = '__NEXT_DATA__']/text()").extract_first()             
                # if script_data:
                #     script_data = json.loads(script_data)
                #     script_data_str = json.dumps(script_data)
                    
                #     pattern = r'"listingId":\s*(\d+)'
                #     match = re.search(pattern, script_data_str)
                    
                #     if match:
                #         listing_id = match.group(1)
                #         item_id = listing_id
                #     else:
                #         item_id = ''
                # else:
                #     item_id = ''   