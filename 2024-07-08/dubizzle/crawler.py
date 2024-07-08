from pipeline import MongoPipeline
from settings import headers, base_url
from parsel import Selector
import requests

class DubizzleCrawler:
    def __init__(self):
        self.headers = headers
        self.mongo_pipeline = MongoPipeline()
        self.collection = self.mongo_pipeline.db["dubizzle_property_urls"]
        self.error_collection = self.mongo_pipeline.db["dubizzle_crawler_error_urls"]
        self.category_collection = self.mongo_pipeline.db["dubizzle_category_urls"]
        
    def fetch_subcategory_urls(self):
        subcategory_urls = self.category_collection.distinct("subcategory_url")
        return subcategory_urls

    def crawl_and_store_urls(self):
        subcategory_urls = self.fetch_subcategory_urls()

        for subcategory_url in subcategory_urls:
            next_page = subcategory_url
            while next_page:
                response = requests.get(next_page, headers=self.headers)
                
                if response.status_code == 200:
                    selector = Selector(response.text)
                    property_urls = selector.xpath("//div[@id='listing-card-wrapper']//a[@type='property']/@href").extract()
                    for property_url in property_urls:
                        full_url = base_url + property_url
                        try:
                            self.collection.insert_one({'subcategory_url': subcategory_url, 'property_url': full_url})
                            print(f"Inserted URL: {full_url} ")
                        except Exception as e:
                            print(f"Error inserting URL {full_url}{e}")
                else:
                    try:
                        self.error_collection.insert_one({'error_url': next_page, 'status_code': response.status_code})
                        print(f"Error inserting URL {next_page} Status Code: {response.status_code}")
                    except Exception as e:
                        print(f"Error inserting error URL {next_page} Status Code: {response.status_code}: {e}")

                next_page = selector.xpath("//a[@data-testid='page-next']/@href").extract_first()
                if next_page:
                    next_page = base_url + next_page
                    print(f"Next page URL: {next_page}")

if __name__ == '__main__':
    crawler = DubizzleCrawler()
    crawler.crawl_and_store_urls()
