import requests
import xml.etree.ElementTree as ET
import logging
from pipeline import get_mongo_collection
from settings import url

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class fressnapfCrawler:
    def __init__(self, sitemap_url):
        self.sitemap_url = sitemap_url
    
    def parse_sitemap(self):
        response = requests.get(self.sitemap_url)
        if response.status_code == 200:
            xml_content = response.content
            root = ET.fromstring(xml_content)

            urls = [url_element.findtext("{http://www.sitemaps.org/schemas/sitemap/0.9}loc") for url_element in root.findall(".//{http://www.sitemaps.org/schemas/sitemap/0.9}url")]

            return urls
        else:
            logging.error("Failed to retrieve sitemap. Status code: %s", response.status_code)
            return None

if __name__ == "__main__":
    sitemap_url = url()
    parser = fressnapfCrawler(sitemap_url)
    parsed_urls = parser.parse_sitemap()

    if parsed_urls:
        collection = get_mongo_collection("fressnapf_site", "product_urls")
        if collection is not None:
            for url in parsed_urls:
                try:
                    collection.insert_one({"url": url})
                    logging.info("Inserted URL: %s", url)
                except Exception as e:
                    logging.error("Failed to insert URL %s: %s", url, e)
    else:
        print("Failed to retrieve sitemap.")