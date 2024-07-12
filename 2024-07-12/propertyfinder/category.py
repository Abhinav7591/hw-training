from pipeline import MongoPipeline
from settings import headers,base_url, main_url
from parsel import Selector
import requests
import logging


class PropertyFinderCategory:
    def __init__(self):
        self.pipeline = MongoPipeline()
        self.collection = self.pipeline.db["Propertyfinder_category_urls"]
        self.headers = headers
        self.base_url = base_url
        self.main_url = main_url

        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    def scrape_category(self):
        response = requests.get(self.main_url, headers=self.headers)
        
        if response.status_code == 200:
            selector = Selector(response.text)
            main_category = selector.xpath("//ul[@class ='styles_list__O4tWB']/li[@aria-current = 'page']/a/@href").extract()

            for sub in main_category:
                category_url = self.base_url + sub
                if 'new-projects' not in category_url and 'find-agent' not in category_url and 'mortgage' not in category_url:
                    logging.info(f"Scraping category URL: {category_url}")
                    response = requests.get(category_url, headers=self.headers)
                    
                    if response.status_code == 200:
                        cat_selector = Selector(response.text)
                        sub_category_urls = cat_selector.xpath("//ul[@class ='styles_desktop_aggregation-links__list__Hx2dE']/li/a")
                        for sub_url in sub_category_urls:
                            full_sub_url = self.base_url + sub_url.xpath(".//@href").extract_first()
                            logging.info(f"Scraping subcategory URL: {full_sub_url}")
                            response = requests.get(full_sub_url, headers=self.headers)
                            property_count = None
                            
                            if response.status_code == 200:
                                sub_selector = Selector(response.text)
                                property_count_text = sub_selector.xpath("//span[@aria-label ='Search results count']/text()").extract_first()
                                if property_count_text:
                                    property_count = property_count_text.split()[0].replace(',', '')
                            else:
                                logging.error(f"Failed to scrape subcategory URL: {full_sub_url} with status code: {response.status_code}")

                            logging.info(f"Category URL: {category_url}, Subcategory URL: {full_sub_url}, Property Count: {property_count}")
                            self.store_to_mongo(category_url, full_sub_url, property_count)
                    else:
                        logging.error(f"Failed to scrape category URL: {category_url} with status code: {response.status_code}")
        else:
            logging.error(f"Failed to scrape main category URL: {main_url} with status code: {response.status_code}")

    def store_to_mongo(self, category_url, sub_category_url, property_count):
        try:
            self.collection.insert_one({
                'category_url': category_url,
                'subcategory_url': sub_category_url,
                'property_count': property_count
            })
            logging.info(f"Storing to MongoDB")
        except Exception as e:
            logging.error(f"Failed to store data to MongoDB with error: {e}")


if __name__ == "__main__":

    scraper = PropertyFinderCategory()
    scraper.scrape_category()
