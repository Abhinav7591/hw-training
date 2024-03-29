import re
import requests
from parsel import Selector
from pymongo import MongoClient
import csv

client = MongoClient("mongodb://localhost:27017/")
db = client["bayut_datas"]
room_urls_collection = db["room_urls"]
parsed_data_collection = db["parsed_data"]

class BayutParser:

    def parse_room_page(self, selector):

        property_id = selector.xpath("//li[span[contains(text() ,'Reference no.')]]/span[2]/text()").get()
        purpose = selector.xpath("//li[span[contains(text() ,'Purpose')]]/span[2]/text()").get()
        type = selector.xpath("//li[span[contains(text() ,'Type')]]/span[2]/text()").get()
        added_on = selector.xpath("//li[span[contains(text() ,'Added on')]]/span[2]/text()").get()
        furnishing = selector.xpath("//li[span[contains(text() ,'Furnishing')]]/span[2]/text()").get()
        price_currency = selector.xpath("//div[@aria-label='Property basic info']//span[@aria-label='Currency']/text()").get()
        price_amount = selector.xpath("//div[@aria-label='Property basic info']//span[@aria-label='Price']/text()").get()
        location = selector.xpath("//div[@aria-label='Property header']/text()").get()

        beds_text = selector.xpath("//div[@aria-label='Property basic info']//span[@aria-label='Beds']/span/text()").get()
        beds = int(re.findall('\d+', beds_text)[0]) if beds_text and re.findall(r'\d+', beds_text) else None

        baths_text = selector.xpath("//div[@aria-label='Property basic info']//span[@aria-label='Baths']/span/text()").get()
        baths = int(re.findall('\d+', baths_text)[0]) if baths_text and re.findall(r'\d+', baths_text) else None

        area = selector.xpath("//div[@aria-label='Property basic info']//span[@aria-label='Area']//span/text()").get()

        nav_items = selector.xpath('//div[@class="_8468d109"]//a//text()').getall()
        breadcrumbs = " > ".join(nav_items)
        
        amenities= selector.xpath("//div[@aria-label='Dialog']//span/text()").getall()
        agent_name = selector.xpath("//a[@aria-label='Agent name']/text()").get()
        image_url = selector.xpath("//picture/img[@aria-label='Cover Photo']/@src").get()
        description_list = selector.xpath("//div[@aria-label='Property description']//span/text()").getall()
        description = ' '.join(description_list)

        room_data = {
            'property_id': property_id,
            'purpose': purpose,
            'type': type,
            'added_on': added_on,
            'furnishing': furnishing,
            'price': {
                'currency': price_currency,
                'amount': price_amount,
            },
            'location': location,
            'bed_bath_size': {
                'bedrooms': beds,
                'bathrooms': baths,
                'size': area,
            },
            'breadcrumbs': breadcrumbs,
            'amenities': amenities,
            'agent_name': agent_name,
            'image_url': image_url,
            'description': description,
        }
        return room_data


if __name__ == "__main__":

    with open('bayut_data.csv', 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['property_id', 'purpose', 'type', 'added_on', 'furnishing',
                    'price_currency', 'price_amount', 'location', 'bedrooms',
                    'bathrooms', 'area', 'breadcrumbs', 'amenities', 'agent_name',
                    'image_url', 'description']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        while room_urls_collection.count_documents({}) > 0:
            room_doc = room_urls_collection.find_one_and_delete({})
            room_url = room_doc["url"]
            html_content = requests.get(room_url).text
            selector = Selector(text=html_content)
            parser = BayutParser()
            room_data = parser.parse_room_page(selector)
            parsed_data_collection.insert_one(room_data)

            filtered_room_data = {}
            for key in fieldnames:
                if key in room_data:
                    filtered_room_data[key] = room_data[key]
                else:
                    filtered_room_data[key] = None
            
            writer.writerow(filtered_room_data)