from settings import headers
from pipeline import MongoPipeline
from parsel import Selector
import requests
import logging
import time


class luluParser:
    def __init__(self):
        self.mongo_pipeline = MongoPipeline()
        self.collection = self.mongo_pipeline.db["luluhypermarket_product_urls"]
        self.parsed_collection = self.mongo_pipeline.db['luluhypermarket_product_data']

        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    def parse_product_page(self, url, price,name):
        timeout = (5, 10)
        try:
            response = requests.get(url, headers=headers,timeout=timeout)           
            if response.status_code == 200:
                selector = Selector(response.text)
                
                url = response.url                
                size_xpath = "//span[text() ='Size']/following-sibling::span/text()"
                brand_xpath = "//div[@class ='product-description']//a[@class ='js-productBrandLink']/text()"
                product_type_xpath = "//span[text()='Type' or text()='Product Type']/following-sibling::span/text()"
                quantity_xpath = "//span[text() ='Content']/following-sibling::span/text()"
                product_summery_xpath = "//h4[text() ='Product Summary']/following-sibling::ul//text()"
                orgin_xpath = "//span[text() ='Country Of Origin']/following-sibling::span/text()"
                
                
                size = selector.xpath(size_xpath).extract_first() or ''
                brand =  selector.xpath(brand_xpath).extract_first()  
                product_type = selector.xpath(product_type_xpath).extract_first() or ''     
                quantity = selector.xpath(quantity_xpath).extract_first() or ''
                
                product_summery = selector.xpath(product_summery_xpath).extract()
                product_summery = ''.join(product_summery)
                
                country_of_orgin =selector.xpath(orgin_xpath).extract_first() or ''

                item = {}
                item['Url'] = url
                item['Size'] = size
                item['Price'] = price
                item['Quantity'] = quantity
                item['Product_Name'] = name
                item['Product_Summary'] = product_summery
                item['Brand'] = brand
                item['Product_Type'] = product_type
                item['Country_Of_Orgin'] = country_of_orgin

                logging.info(item)
                try:
                    self.parsed_collection.insert_one(item)
                    logging.info("item information saved successfully!")
                except Exception as e:
                    logging.error(f"Failed to save item information: {str(e)}")

            else:
                logging.error(f"Failed to fetch URL: {url}. Status code: {response.status_code}")
        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed: {str(e)}")


    def parse_urls(self, fixed_delay=5):
        unique_urls = self.collection.distinct("url")
        for url in unique_urls:
            product_doc = self.collection.find_one_and_delete({"url": url})
            if product_doc:
                price = product_doc["price"]
                name = product_doc["product_name"]
                self.parse_product_page(url, price,name)
                time.sleep(fixed_delay)
                
if __name__ == "__main__":
    parser = luluParser()
    parser.parse_urls()