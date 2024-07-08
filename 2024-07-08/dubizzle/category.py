from settings import headers, base_url, category_url
from pipeline import MongoPipeline
from parsel import Selector
from pymongo import errors
import logging
import requests
import re

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DubizzleScraper:
    def __init__(self):
        self.base_url = base_url
        self.category_url = category_url
        self.headers = headers    
        self.mongo_pipeline = MongoPipeline()
        self.collection_category = self.mongo_pipeline.db["dubizzle_category_urls"]
        self.main_categories = {
            'For Rent': 'https://uae.dubizzle.com/en/property-for-rent/',
            'For Sale': 'https://uae.dubizzle.com/en/property-for-sale/'
        }

    def scrape_categories(self):
        try:
            response = requests.get(self.category_url, headers=self.headers)
            selector = Selector(response.text)
            category_elements = selector.xpath("//a[@data-fnid='category_property-for-rent']/following-sibling::div/ul/li")

            categories_data = []
            for category_elem in category_elements:
                category_url = category_elem.xpath(".//a/@href").extract_first()
                category_name = category_elem.xpath(".//a/text()").extract_first()

                if category_url and category_name:
                    full_url = self.base_url + category_url
                    response = requests.get(full_url, headers=headers)
                    category_selector = Selector(response.text)
                    property_count = category_selector.xpath("//h1[@data-testid='page-title']/span/text()").extract()
                    data_string = ''.join(property_count)
                    property_count = re.search(r'\d{1,3}(,\d{3})*', data_string).group() if re.search(r'\d{1,3}(,\d{3})*', data_string) else None

                    if 'property-for-rent' in category_url:
                        main_category_name = 'For Rent'
                        main_category_url = self.main_categories['For Rent']
                    elif 'property-for-sale' in category_url:
                        main_category_name = 'For Sale'
                        main_category_url = self.main_categories['For Sale']
                    else:
                        main_category_name = None
                        main_category_url = None

                    if main_category_name and main_category_url and category_name != main_category_name:
                        categories_data.append({
                            'main_category_url': main_category_url,
                            'main_category_name': main_category_name,
                            'subcategory_url': full_url,
                            'subcategory_name': category_name,
                            'property_count': property_count
                        })

            return categories_data

        except Exception as e:
            logging.error(f"Error occurred while scraping categories: {e}")
            return []

    def insert_into_mongodb(self, categories_data):
        try:
            for data in categories_data:
                self.collection_category.insert_one(data)
                logging.info(f"Inserted: {data}")
        except errors.DuplicateKeyError:
            logging.warning("Duplicate entry found for subcategory_url.")
        except Exception as e:
            logging.error(f"Error occurred while inserting into MongoDB: {e}")

if __name__ == '__main__':
    scraper = DubizzleScraper()
    categories_data = scraper.scrape_categories()
    scraper.insert_into_mongodb(categories_data)
