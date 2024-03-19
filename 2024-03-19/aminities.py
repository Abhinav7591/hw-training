import requests
from scrapy import Selector

url = "https://www.bayut.com/property/details-8724106.html"


response = requests.get(url)

if response.status_code == 200:
    selector = Selector(text=response.text)

    amenities = selector.xpath('//div[@id="property-amenity-dialog"]/ul/div')

    for amenity in amenities:
        amenity_heading = amenity.xpath('./div[1]/text()').get()
        amenity_values = amenity.xpath('./div[2]//span')
        values_data = []

        for val in amenity_values:
            value= ''.join(val.xpath('./text()').getall())
            values_data.append(value)

        amenities_dict = {}
        print(amenity_heading, '-', values_data)
        amenities_dict.update({amenity_heading: values_data})
