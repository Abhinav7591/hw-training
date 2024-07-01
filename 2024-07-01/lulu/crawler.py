from settings import base_url,category_base_url,headers
from pipeline import MongoPipeline
from pymongo import errors
from parsel import Selector
import requests

class LuluCrawler:
    def __init__(self):
        self.base_url = base_url
        self.category_base_url = category_base_url
        self.headers = headers
        self.mongo_pipeline = MongoPipeline()
        self.collection = self.mongo_pipeline.db["luluhypermarket_product_urls"]
        self.collection_category = self.mongo_pipeline.db["luluhypermarket_category_urls"]

    def scrape_urls(self):
        response = requests.get(self.category_base_url, headers=self.headers)
        if response.status_code != 200:
            print(f"Failed to retrieve {self.category_base_url} Status Code {response.status_code}")
            return

        selector = Selector(response.text)
        category  = selector.xpath("//ul[@class ='js-navigationBar']/li[@class ='first-level with-dropdown']")

        for main_category in category:
            main_category_url = main_category.xpath(".//div[@class ='container-col-title']/a/@href").extract()
            sub_category = main_category.xpath(".//div[@class ='container-col-title']/following-sibling::div/ul/li")
            
            for sub_category_url,main_category_urls in zip(sub_category,main_category_url):
                             
                main_category_url = self.base_url+main_category_urls
                
                sub_category_urls = sub_category_url.xpath(".//a/@href").extract_first()
                
                if sub_category_urls != None and 'gift-card' not in sub_category_urls:
                    subcategory_url =  self.base_url + sub_category_urls
                    item = {
                            'category_urls': main_category_url,
                            'sub_category_url': subcategory_url
                        }
                    self.collection_category.insert_one(item)
                    
                    page = 0
                    while True:
                        all_category_url = f"{subcategory_url}/results-display?q=%3Adiscount-desc&sort=&page={page}"
                        response = requests.get(all_category_url, headers=self.headers)
                        
                        if response.status_code != 200:
                            print(f"Failed to retrieve {all_category_url} Status Code {response.status_code}")
                            break
                        
                        product_selector = Selector(response.text)
                        product_urls = product_selector.xpath("//a[@class='js-gtm-product-link']/@href").extract()
                        product_names =product_selector.xpath("//div[@class ='product-content']//h3/text()").extract()
                        product_prices = product_selector.xpath("//p[@class='product-price has-icon']/span[not(@class='old-price')]/text()").extract()

                        if not product_urls:
                            break
                        
                        for product_url, product_name,product_prices in zip(product_urls, product_names,product_prices):
                            url = self.base_url + product_url
                            product_name = product_name.strip()
                            if url:
                                try:
                                    self.collection.insert_one({'url': url, 'product_name': product_name, 'price': product_prices})
                                    print(f"Inserted URL: {url}")
                                except errors.DuplicateKeyError:
                                    print(f"URL already exists: {url}")             
                        page += 1
                
if __name__ == "__main__":
    
    scraper = LuluCrawler()
    scraper.scrape_urls()
