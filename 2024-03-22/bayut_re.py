import re
import requests
from parsel import Selector

class BayutScraper:
    def __init__(self, start_url):
        self.start_url = start_url
        self.base_url = "https://www.bayut.com"
        self.scraped_count = 0

    def fetch_page(self, url):
        response = requests.get(url)
        if response.status_code == 200:
            return response.text
        else:
            print(f"Failed to fetch page {url}")
            return None

    def do_scraping(self):
        html_content = self.fetch_page(self.start_url)
        if html_content:
            selector = Selector(html_content)
            self.parse(selector)

    def parse(self, selector):
        rooms = selector.xpath('//article[contains(@class, "ca2f5674")]')
        for room in rooms:
            next_page_behind = room.xpath('.//a/@href').get()
            next_page_url = self.base_url + next_page_behind
            room_html = self.fetch_page(next_page_url)
            if room_html:
                room_selector = Selector(room_html)
                room_data = self.parse_room_page(room_selector)
                print(room_data)
                self.scraped_count += 1
            else:
                print(f"Failed to fetch room details for {next_page_url}")

        next_page = selector.xpath('//a[@class="b7880daf" and @title="Next"]/@href').get()
        if next_page:
            next_page_url = self.base_url + next_page
            next_page_html = self.fetch_page(next_page_url)
            if next_page_html:
                next_page_selector = Selector(next_page_html)
                self.parse(next_page_selector)

    def parse_room_page(self, selector):

        property_id_match = re.search(r'<span class="_3af7fa95">Reference no.</span><span class="_812aa185" aria-label="Reference">([^<]+)</span>',selector.get())
        property_id = property_id_match.group(1) if property_id_match else None

        purpose_matching = re.search(r'<span class="_3af7fa95">Purpose</span><span class="_812aa185" aria-label="Purpose">([^<]+)</span>',selector.get())
        purpose = purpose_matching.group(1) if purpose_matching else None

        type_match = re.search(r'<span class="_3af7fa95">Type</span><span class="_812aa185" aria-label="Type">([^<]+)</span>',selector.get())
        type = type_match.group(1) if type_match else None

        added_on = selector.xpath("//li[span[contains(text() ,'Added on')]]/span[2]/text()").get()
        furnishing = selector.xpath("//li[span[contains(text() ,'Furnishing')]]/span[2]/text()").get()

        currency_match = re.search(r'<span class="e63a6bfb" aria-label="Currency">([^<]+)</span>', selector.get())
        price_currency = currency_match.group(1) if currency_match else None

        price_amount_match = re.search(r'<span class="_105b8a67" aria-label="Price">(\d+,\d+)</span>', selector.get())
        price_amount = price_amount_match.group(1) if price_amount_match else None

        location = selector.xpath("//div[@aria-label='Property header']/text()").get()

        beds_text = selector.xpath("//div[@aria-label='Property basic info']//span[@aria-label='Beds']/span/text()").get()
        beds = int(re.findall('\d+', beds_text)[0]) if beds_text and re.findall(r'\d+', beds_text) else None

        baths_text = selector.xpath("//div[@aria-label='Property basic info']//span[@aria-label='Baths']/span/text()").get()
        baths = int(re.findall('\d+', baths_text)[0]) if baths_text and re.findall(r'\d+', baths_text) else None

        area_pattern = re.search(r'<span class="cfe8d274" aria-label="Area"><span class="fc2d1086"><span>([\d,]+ sqft)</span>',selector.get())
        area = area_pattern.group(1) if area_pattern else None


        nav_items = selector.xpath('//div[@class="_8468d109"]//a//text()').getall()
        breadcrumbs = " > ".join(nav_items)
      
        amenities= selector.xpath("//div[@aria-label='Dialog']//span/text()").getall()

        agent_name_match = re.search(r'<span class="_63b62ff2"><a .*>(.*)</a></span>',selector.get())
        agent_name = agent_name_match.group(1) if agent_name_match else None
        
        image_url = selector.xpath("//picture/img[@aria-label='Cover Photo']/@src").get()
        description = selector.xpath("//div[@aria-label='Property description']//span/text()").getall()

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
    start_url = "https://www.bayut.com/to-rent/property/dubai/"
    scraper = BayutScraper(start_url)
    scraper.do_scraping()

    # print(f"Scraped {scraper.scraped_count} rooms.")
