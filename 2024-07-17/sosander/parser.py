from settings import HEADERS
from pipeline import MongoPipeline
from parsel import Selector
import requests
import logging
import json
import time
import re


class Parser:
    def __init__(self):
        self.mongo_pipeline = MongoPipeline()
        self.collection = self.mongo_pipeline.db["sosander_product_url"]
        self.parsed_collection = self.mongo_pipeline.db['sosander_product_data']
        self.error_collection = self.mongo_pipeline.db["sosander_parser_error_urls"]

        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        
        
    def start(self, fixed_delay=5):
        for item in self.collection.find():
            url = item.get('product_url', '')
            if url:
                data_url = self.parsed_collection.find_one({'url': url})
                if not data_url:
                    try:
                        response = requests.get(url, headers=HEADERS)
                        response.raise_for_status()
                        selector = Selector(response.text)
                        self.parser(url, selector)
                        logging.info(f"Successfully scraped data")

                    except requests.exceptions.RequestException as e:
                        logging.error(f"Error fetching URL {url}: {e}")
                        existing_error = self.error_collection.find_one({"failed_url": url})
                        if not existing_error:
                            try:
                                self.error_collection.insert_one({"failed_url": url, "error": str(e)})
                            except Exception as e:
                                logging.error(f"Duplicate key error for URL: {url}")
                    time.sleep(fixed_delay)
        logging.info(f"Scraped {self.parsed_collection.count_documents({})} product URLs")


    def parser(self, url, selector):
        
        product_name = selector.xpath("//span[@data-ui-id ='page-title-wrapper']/text()").extract_first()       
        selling_price =  selector.xpath("//span[@data-price-type= 'finalPrice']/span[@class ='price']/text()").extract_first() 
        regular_price = selector.xpath("//div[@class ='product-info-price']//span[@data-price-type ='oldPrice']//text()").extract_first() or ''
        
        discount_text = selector.xpath("//div[@class='product-info-price']//span[@class='price-container discount-percentage']/text()").extract_first()
        percentage_discount = ''
        promotion_description = ''
        if discount_text:
            match = re.search(r'(\d+%)', discount_text)
            if match:
                promotion_description = match.group(0) 
                percentage_discount = match.group(1)[:-1]  
 
        
        script_content = selector.xpath("//script[@type = 'application/ld+json'][5]/text()").extract_first()
        script_data = json.loads(script_content)
        unique_id =  script_data.get('productID','')
        product_unique_key = unique_id + 'P'
        description =  script_data.get('description','')
        brand = script_data.get('brand',{}).get('name','')
        currency = script_data.get('offers',{}).get('priceCurrency','')
        
        
        script_breadcrumb = selector.xpath("//script[@type = 'application/ld+json'][4]/text()").extract_first()
        breadcrumb_data = json.loads(script_breadcrumb)
        breadcrumb = breadcrumb_data.get('itemListElement',[])
        breadcrumb_names = []
        for item in breadcrumb:
            breadcrumbs = item.get('item',{}).get('name','')
            if breadcrumbs: 
                breadcrumb_names.append(breadcrumbs)
        clean_breadcrumbs = ' > '.join(breadcrumb_names)
        
        
        
        img_script = selector.xpath("//script[@type='text/x-magento-init'][contains(text(), '[data-gallery-role=gallery-placeholder]')]").extract_first()
        pattern = r'"full":"(https:\\/\\/cdn\.sosandar\.com\\/media\\/catalog\\/product\\/cache\\/[^"]+)"'
        matches = re.findall(pattern, img_script)
        full_urls = [url.replace('\/', '/') for url in matches]
        images = [url for url in full_urls if 'w/e/web' in url]
        
        image_url_1 = images[0] if len(images) >= 1 else ''
        file_name_1 = unique_id + '_1.png' if image_url_1 else ''

        image_url_2 = images[1] if len(images) >= 2 else ''
        file_name_2 = unique_id + '_2.png' if image_url_2 else ''

        image_url_3 = images[2] if len(images) >= 3 else ''
        file_name_3 = unique_id + '_3.png' if image_url_3 else ''

        image_url_4 = images[3] if len(images) >= 4 else ''
        file_name_4 = unique_id + '_4.png' if image_url_4 else ''

        image_url_5 = images[4] if len(images) >= 5 else ''
        file_name_5 = unique_id + '_5.png' if image_url_5 else ''

        image_url_6 = images[5] if len(images) >= 6 else ''
        file_name_6 = unique_id + '_6.png' if image_url_6 else ''
        
        

        item = {}
        item['url'] = url
        item['unique_id'] = unique_id
        item['product_name'] = product_name
        item['brand'] = brand
        item['currency'] = currency
        item['selling_price'] = selling_price
        item['regular_price'] = regular_price
        item['breadcrumb'] = clean_breadcrumbs
        item['percentage_discount'] =  percentage_discount
        item['promotion_description'] =  promotion_description       
        item['file_name_1'] = file_name_1
        item['image_url_1'] = image_url_1
        item['file_name_2'] = file_name_2
        item['image_url_2'] = image_url_2
        item['file_name_3'] = file_name_3
        item['image_url_3'] = image_url_3
        item['file_name_4'] = file_name_4
        item['image_url_4'] = image_url_4
        item['file_name_5'] = file_name_5
        item['image_url_5'] = image_url_5
        item['file_name_6'] = file_name_6
        item['image_url_6'] = image_url_6
        item['description'] = description
        item['product_unique_key'] = product_unique_key

        logging.info(item)
        self.parsed_collection.insert_one(item)


if __name__ == "__main__":
    parse = Parser()
    parse.start()
