import json
import requests
from scrapy import Selector

url = "https://www.bayut.com/property/details-8724106.html"

response = requests.get(url)

if response.status_code == 200:
    selector = Selector(text=response.text)
    javascript_data = selector.xpath('//script/text()').get()

    if javascript_data:
        start_index = javascript_data.find('{')
        end_index = javascript_data.rfind('}')

        json_string = javascript_data[start_index:end_index+1]

        data = json.loads(json_string)
        # print(json.dumps(data, indent=4))
        
        property_data = {
            'url': data.get('starting_page_url'),
            'reference_no': data.get('reference_id'),
            'property_name': data.get('listing_title'),
            'price': data.get('price'),
            'currency': data.get('currency_unit'),
            'type': data.get('property_type'),
            'purpose': data.get('website_section'),
            'furnishing': data.get('furnishing_status'),
            'baths': data.get('property_baths_list'),
            'bedrooms': data.get('property_beds_list'),
            'area': data.get('property_land_area'),
            'location': f"{data.get('loc_3_name').strip(';')},{data.get('loc_2_name').strip(';')},{data.get('loc_1_name').strip(';')}",
            'latitude': data.get('latitude'),
            'longitude': data.get('longitude')
        }
    
        for key, value in property_data.items():
            print(f"{key}: {value}")
