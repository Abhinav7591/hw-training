from settings import parser_header
from pipeline import MongoPipeline
from parsel import Selector
from pymongo import errors
import requests
import logging
import time
import json

class carrefourParser:
    def __init__(self):
        self.mongo_pipeline = MongoPipeline()
        self.collection = self.mongo_pipeline.db["carrefouruae_product_urls"]
        self.parsed_collection = self.mongo_pipeline.db['carrefouruae_product_data']
        self.error_collection = self.mongo_pipeline.db["carrefouruae_parsed_error_urls"]

        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    def parse_product_page(self, url):
        timeout = (5, 10)
        try:
            response = requests.get(url, headers=parser_header,timeout=timeout)           
            if response.status_code == 200:
                selector = Selector(response.text)
                
                url = response.url  
                script_datas = "//script[@id ='__NEXT_DATA__']/text()"
                price_xpath = "//div[@data-testid ='description-box']//h2/text()"
                product_name_xpath = "//div[@data-testid ='description-box']//h1/text()" 
                product_summery_xpath = "//div[text() = 'highlights']/following-sibling::div//text()"
                brand_xpath = "//div[@data-testid ='description-box']//a/text()"
                orgin_xpath = "//div[contains(text(),'Origin')]/following-sibling::div/text()"
                
                script_data = selector.xpath(script_datas).extract_first()
                script_data = json.loads(script_data)
                size = script_data.get('props', {}).get('initialProps', {}).get('pageProps', {}).get('initialData', {}).get('products', [{}])[0].get('attributes', {}).get('size','')
                product_type = script_data.get('props', {}).get('initialProps', {}).get('pageProps', {}).get('initialData', {}).get('products', [{}])[0].get('attributes', {}).get('productType','')
                
                price = selector.xpath(price_xpath).extract()
                price = next((item for item in price if item.replace('.', '', 1).isdigit()), '')
                
                product_name = selector.xpath(product_name_xpath).extract_first() 
                
                product_summery = selector.xpath(product_summery_xpath).extract()
                product_summery = ''.join(product_summery)
                
                brand = selector.xpath(brand_xpath).extract_first() or ''
                country_of_orgin = selector.xpath(orgin_xpath).extract_first() or ''           

                item = {}
                item['Url'] = url
                item['Size'] = size
                item['Price'] = price
                # item['Quantity'] = quantity
                item['Product_Name'] = product_name
                item['Product_Summary'] = product_summery
                item['Brand'] = brand
                item['Product_Type'] = product_type
                item['Country_Of_Orgin'] = country_of_orgin

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
        unique_urls = self.collection.distinct("url")
        for url in unique_urls:
            product_doc = self.collection.find_one_and_delete({"url": url})
            if product_doc:
                self.parse_product_page(url)
                time.sleep(fixed_delay)
                
if __name__ == "__main__":
    parser = carrefourParser()
    parser.parse_urls()