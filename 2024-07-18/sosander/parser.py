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
                        logging.info(f"Successfully scraped data from {url}")

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
        
        product_name_xpath = "//span[@data-ui-id ='page-title-wrapper']/text()"
        selling_price_xpath = "//span[@data-price-type= 'finalPrice']/span[@class ='price']/text()"
        regular_price_xpath = "//div[@class ='product-info-price']//span[@data-price-type ='oldPrice']//text()"
        discount_xpath = "//div[@class='product-info-price']//span[@class='price-container discount-percentage']/text()"
        size_xpath = "//div[@id ='revealSizeguide']/table//th/text()"
        color_xpath = "//ul[@class ='related-alternatives_list']/li/a/span/text()"
        script_content_xpath = "//script[@type = 'application/ld+json'][5]/text()"
        script_breadcrumb_xpath = "//script[@type = 'application/ld+json'][4]/text()"
        image_script_xpath = "//script[@type='text/x-magento-init'][contains(text(), '[data-gallery-role=gallery-placeholder]')]"
        
        product_name = selector.xpath(product_name_xpath).extract_first()   
            
        selling_price_data =  selector.xpath(selling_price_xpath).extract_first() 
        if selling_price_data:
            cleaned_sprice = re.sub(r'[^\d.]', '', selling_price_data)
            selling_price = float(cleaned_sprice)
        else:
            selling_price = '' 
            
            
        regular_price_data = selector.xpath(regular_price_xpath).extract_first()
        if regular_price_data:
            cleaned_rprice = re.sub(r'[^\d.]', '', regular_price_data)
            regular_price = float(cleaned_rprice)
        else:
            regular_price = ''
        
        
        discount_text = selector.xpath(discount_xpath).extract_first()
        percentage_discount = ''
        promotion_description = ''
        if discount_text:
            match = re.search(r'(\d+%)', discount_text)
            if match:
                promotion_description = match.group(0) 
                percentage_discount = match.group(1)[:-1]  
                
        size_data = selector.xpath(size_xpath).extract()
        size = [sizes for sizes in size_data if sizes.isdigit()]
        
        colors = selector.xpath(color_xpath).extract()
        color = [c.strip() for c in colors if c.strip()]
        
        description = ''
        unique_id = ''
        brand = ''
        currency = ''
        script_content = selector.xpath(script_content_xpath).extract_first()

        if script_content:
            try:
                script_data = json.loads(script_content)
                unique_id = script_data.get('sku', '')
                description = script_data.get('description', '')
                brand = script_data.get('brand', {}).get('name', '')
                currency = script_data.get('offers', {}).get('priceCurrency', '')
            except json.JSONDecodeError as e:
                logging.error(f"Error decoding JSON for URL {url}: {e}")
                script_data = {}
        else:
            logging.info(f"No script content found for URL {url}")
            script_data = {}
        
        
        script_breadcrumb = selector.xpath(script_breadcrumb_xpath).extract_first()
        if script_breadcrumb:
            breadcrumb_data = json.loads(script_breadcrumb)
            breadcrumb = breadcrumb_data.get('itemListElement',[])
            breadcrumb_names = []
            for item in breadcrumb:
                breadcrumbs = item.get('item',{}).get('name','')
                if breadcrumbs: 
                    breadcrumb_names.append(breadcrumbs)
            clean_breadcrumbs = ' > '.join(breadcrumb_names)
        else:
            clean_breadcrumbs = ''
            breadcrumb_names = []
        
        product_hierarchy_level1 = breadcrumb_names[0] if len(breadcrumb_names) >= 1 else ''
        product_hierarchy_level2 = breadcrumb_names[1] if len(breadcrumb_names) >= 2 else ''
        product_hierarchy_level3 = breadcrumb_names[2] if len(breadcrumb_names) >= 3 else ''
        product_hierarchy_level4 = breadcrumb_names[3] if len(breadcrumb_names) >= 4 else ''
        product_hierarchy_level5 = breadcrumb_names[4] if len(breadcrumb_names) >= 5 else ''
        product_hierarchy_level6 = breadcrumb_names[5] if len(breadcrumb_names) >= 6 else ''
        product_hierarchy_level7 = breadcrumb_names[6] if len(breadcrumb_names) >= 7 else '' 
        
             
        img_script = selector.xpath(image_script_xpath).extract_first()
        if img_script:
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
        else:
            images = []
            image_url_1 = ''
            file_name_1 = ''
            image_url_2 = ''
            file_name_2 = ''
            image_url_3 = ''
            file_name_3 = ''
            image_url_4 = ''
            file_name_4 = ''
            image_url_5 = ''
            file_name_5 = ''
            image_url_6 = ''
            file_name_6 = ''
        
        composition_pattern = r"Product composition:\s*(.*?)\s*(?:Product length|Product code|Care information|$)"
        care_info_pattern =  r"Care information:\s*(.*?)\s*(?:Product length|Product code|$)"
        length_pattern = r"Product length:\s*(.*?)\s*(?:Product composition|Product code|Care information|$)"
        heel_height_pattern = r"Heel Height:\s*(.*?)\s*(?:Product composition|Product code|Care information|$)"
        
        composition_match = re.search(composition_pattern, description)
        if composition_match:
            product_composition = composition_match.group(1)
        else:
            product_composition = ''

        care_info_match = re.search(care_info_pattern, description)
        if care_info_match:
            care_information = care_info_match.group(1)
        else:
            care_information = ''
                
        length_match = re.search(length_pattern, description)
        if length_match:
            product_length = length_match.group(1).strip()
        else:
            product_length = ''
            
        heel_height_match = re.search(heel_height_pattern, description)
        if heel_height_match:
            heel_height = heel_height_match.group(1).strip()
        else:
            heel_height = ''

        item = {}
        item['url'] = url
        item['unique_id'] = unique_id
        item['product_name'] = product_name
        item['brand'] = brand
        item['currency'] = currency
        item['selling_price'] = selling_price
        item['regular_price'] = regular_price
        item['percentage_discount'] =  percentage_discount
        item['promotion_description'] =  promotion_description  
        item['breadcrumb'] = clean_breadcrumbs
        item['product_hierarchy_level1'] = product_hierarchy_level1 
        item['product_hierarchy_level2'] = product_hierarchy_level2  
        item['product_hierarchy_level3'] = product_hierarchy_level3  
        item['product_hierarchy_level4'] = product_hierarchy_level4  
        item['product_hierarchy_level5'] = product_hierarchy_level5  
        item['product_hierarchy_level6'] = product_hierarchy_level6  
        item['product_hierarchy_level7'] = product_hierarchy_level7     
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
        item['variants'] = size
        item['color'] = color
        item['care_instructions'] = care_information
        item['material_composition'] = product_composition
        item['clothing_length'] = product_length
        item['heel_height'] = heel_height

        logging.info(item)
        self.parsed_collection.insert_one(item)
        
if __name__ == "__main__":
    parse = Parser()
    parse.start()
