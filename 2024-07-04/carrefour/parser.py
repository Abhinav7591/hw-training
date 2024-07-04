from settings import parser_header,promotionapi_header
from pipeline import MongoPipeline
from parsel import Selector
from pymongo import errors
import requests
import logging
import time
import json
import re

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
                quantity_xpath = "//div[@class ='css-g4iap9']/text()"
                price_xpath = "//div[@data-testid ='description-box']//h2/text()"
                product_name_xpath = "//div[@data-testid ='description-box']//h1/text()" 
                product_summery_xpath = "//div[text() = 'highlights']/following-sibling::div//text()"
                description_xpath = "//h2[text() = 'DESCRIPTION']/following-sibling::div//text()"
                brand_xpath = "//div[@data-testid ='description-box']//a/text()"
                orgin_xpath = "//div[contains(text(),'Origin')]/following-sibling::div/text()"
                breadcrumbs_xpath = "//div[@class ='css-1wzbwlf']/div/a/text()"
                
                
                quantity_data = selector.xpath(quantity_xpath).extract()
                quantity = ''
                for item in quantity_data:
                    if re.search(r'\d', item):
                        quantity = int(item)
                        break
                
                product_name = selector.xpath(product_name_xpath).extract_first() 
                
                script_data = selector.xpath(script_datas).extract_first()
                script_data = json.loads(script_data)
                initial_data = script_data.get('props', {}).get('initialProps', {}).get('pageProps', {}).get('initialData', {})
                package_size = initial_data.get('products', [{}])[0].get('attributes', {}).get('size','')
                
                price = selector.xpath(price_xpath).extract()
                price = next((item for item in price if item.replace('.', '', 1).isdigit()), '')
                
                orgin = selector.xpath(orgin_xpath).extract_first() or ''
                              
                description = selector.xpath(description_xpath).extract()
                description = ''.join(description)
                product_summery = selector.xpath(product_summery_xpath).extract()
                product_summery = ''.join(product_summery)
                if description:
                    product_description = description
                else:
                    product_description = product_summery
                
                breadcrumbs = selector.xpath(breadcrumbs_xpath).extract()
                product_type= breadcrumbs[-2]
                
                brand = selector.xpath(brand_xpath).extract_first() or ''
                
                products = initial_data.get('products', [])
                sellers = []
                for product in products:
                    offers = product.get('offers', [])
                    for offer in offers:
                        shop = offer.get('shop', '')
                        if shop:
                            sellers.append(shop)
                            
                
                cat_id = initial_data.get('products', [{}])[0].get('attributes', {}).get('categoriesHierarchy',[{}])[0].get('id','')
                prod_id = initial_data.get('products', [{}])[0].get('id','')
                
                api_url = f'https://www.carrefouruae.com/v1/frame/pdp/promotion-banners?categoryCode={cat_id}&productCode={prod_id}'
                response = requests.get(api_url,headers=promotionapi_header )
                json_data = response.json()
                offers_promotions = json_data.get('data',{}).get('promotionBanners',[{}])[0].get('popUpDescription','')
                offers_promotions = offers_promotions.replace('\n','') if offers_promotions else ''
                
                           

                item = {}
                item['Url'] = url
                item['Quantity'] = quantity
                item['Product_Name'] = product_name
                item['package_size'] = package_size
                item['Price'] = price
                item['Orgin'] = orgin
                item['Item_Description'] = product_description
                item['Sellers'] = sellers
                item['Product_Type'] = product_type
                item['Brand'] = brand
                item['Offers_Promotions'] = offers_promotions
                
        
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
            url = item.get('url', '')
            if url:
                data_url = self.parsed_collection.find_one({'Url': url})
                if not data_url:
                    self.parse_product_page(url)
                    time.sleep(fixed_delay)
                
if __name__ == "__main__":
    parser = carrefourParser()
    parser.parse_urls()