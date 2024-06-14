import requests
from parsel import Selector
import logging
import re
import json
from settings import headers
from pipeline import MongoPipeline
import time
from datetime import datetime

class fressnapfParser:
    def __init__(self):
        self.mongo_pipeline = MongoPipeline()
        self.collection = self.mongo_pipeline.db["product_urls"]
        self.parsed_collection = self.mongo_pipeline.db['product_details']

        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

        self.request_count = 0
        self.max_requests = 6000

    def parse_product_page(self, url):
        timeout = (5, 10)
        try:
            response = requests.get(url, headers=headers,timeout=timeout)
            self.request_count += 1
            
            if response.status_code == 200:
                selector = Selector(response.text)
                
                
                json_ld_data = selector.xpath('(//script[@type="application/ld+json"])[2]/text()').get()
                if json_ld_data:
                    data = json.loads(json_ld_data)
                    graph = data.get('@graph', [])
                    if len(graph) > 1:
                        product_name = graph[1].get('name', '')
                        unique_id = graph[1].get('sku', '')
                        product_unique_key =  unique_id + 'P'
                        offers = graph[1].get('offers', [{}])[0]
                        currency = offers.get('priceCurrency', '')
                        selling_price = offers.get('price', '')                            
                        instock = offers.get('availability', '')
                        if 'instock' in instock.lower():
                            instock = True
                        else:
                            instock = False
                            
                        promotion_valid_upto = offers.get('priceValidUntil', '')                       
                        aggregate_rating = graph[1].get('aggregateRating', {})
                        rating = aggregate_rating.get('ratingValue', '') 
                        review  = aggregate_rating.get('reviewCount', '')
                        if rating:
                            rating = str(float(rating)).rstrip('0').rstrip('.')
                             
                
                breadcrumbs_data = selector.xpath("//nav[@aria-label ='breadcrumbs']//text()").extract()
                breadcrumb = []
                for crumb in breadcrumbs_data:
                    crumb = crumb.strip()
                    if crumb and crumb != '/':
                        breadcrumb.append(crumb)
                breadcrumbs = ' > '.join(breadcrumb)
                product_hierarchy_level1 = breadcrumb[0]
                product_hierarchy_level2 = breadcrumb[1] if len(breadcrumb) >= 2 else ''
                product_hierarchy_level3 = breadcrumb[2] if len(breadcrumb) >= 3 else ''
                product_hierarchy_level4 = breadcrumb[3] if len(breadcrumb) >= 4 else ''
                product_hierarchy_level5 = breadcrumb[4] if len(breadcrumb) >= 5 else ''
                product_hierarchy_level6 = breadcrumb[5] if len(breadcrumb) >= 6 else ''
                product_hierarchy_level7 = breadcrumb[6] if len(breadcrumb) >= 7 else '' 
                
                
                brand = selector.xpath('//th[text()="Márka"]/following-sibling::td/p/a/text()').extract_first() or ''
                
                packaging = selector.xpath('//th[text()="Kiszerelés"]/following-sibling::td/p/a/text()').extract_first() or ''
                
                age_recommendations = selector.xpath('//th[text()="Életszakasz"]/following-sibling::td/p/a/text()').extract_first() or ''
                
                special_information = selector.xpath('//th[text()="Speciális igények"]/following-sibling::td/p/a/text()').extract_first() or ''
                
                tasting_note = selector.xpath('//th[text()="Íz"]/following-sibling::td/p/a/text()').extract_first() or ''
                
                color = selector.xpath('//th[text()="Szín"]/following-sibling::td/p/a/text()').extract_first() or ''
                
                material = selector.xpath('//th[text()="Anyag"]/following-sibling::td/p/a/text()').extract_first() or ''
                
                size = selector.xpath('//th[text()="Méret"]/following-sibling::td/p/a/text()').extract_first() or ''
                
                     
                regular_price = selector.xpath("//p[@class ='price']/del/span[@class ='woocommerce-Price-amount amount']/bdi/text()").extract_first()
                regular_price = regular_price.replace('\xa0', '').replace(' ', '') if regular_price else ''
                
                price_per_unit = selector.xpath("//span[@class ='egysegar']/text()").extract_first()
                price_per_unit = price_per_unit.strip() if price_per_unit else ''
 
                
                promotion_description = selector.xpath("//p[@class ='saving_total_price']//text()").extract()
                promotion_description = ''.join(promotion_description).replace('\xa0', '')
 
                
                price_valid_from_date = selector.xpath("//p[@class ='permanently_low_price']/span/text()").extract_first()
                if price_valid_from_date:
                    try:
                        date_object = datetime.strptime(price_valid_from_date, '%Y.%m.%d-től')
                        price_valid_from = date_object.strftime('%Y-%m-%d')
                    except ValueError as e:
                        print(f"Error parsing date: {e}")
                        price_valid_from = ''
                else:
                    price_valid_from = ''
                    
                    
                product_description = selector.xpath("//div[@id ='tab-description']//p/text()").extract()
                product_description = ''.join(product_description).strip().replace('\n', ' ').replace('\r', ' ').replace('\xa0', ' ')
                product_description = re.sub(' +', ' ', product_description)
                
                
                img_tags = selector.xpath("//div[contains(@class, 'woocommerce-product-gallery images wpgs-wrapper')]//img")
                zoom_image_urls = []
                if img_tags:
                    zoom_image_urls.extend(img_tags.xpath("@data-zoom-image").getall())
                else:
                    img_tag = selector.xpath("//div[@class='woocommerce-product-gallery images wpgs-wrapper  wpgs-no-gallery-images']//img")
                    zoom_image_url = img_tag.xpath("@data-zoom-image").get()
                    if zoom_image_url:
                        zoom_image_urls.append(zoom_image_url)

                image_url_1 = zoom_image_urls[0] if len(zoom_image_urls) >= 1 else ''
                file_name_1 = unique_id + '_1.png' if image_url_1 else ''

                image_url_2 = zoom_image_urls[1] if len(zoom_image_urls) >= 2 else ''
                file_name_2 = unique_id + '_2.png' if image_url_2 else ''

                image_url_3 = zoom_image_urls[2] if len(zoom_image_urls) >= 3 else ''
                file_name_3 = unique_id + '_3.png' if image_url_3 else ''

                image_url_4 = zoom_image_urls[3] if len(zoom_image_urls) >= 4 else ''
                file_name_4 = unique_id + '_4.png' if image_url_4 else ''

                image_url_5 = zoom_image_urls[4] if len(zoom_image_urls) >= 5 else ''
                file_name_5 = unique_id + '_5.png' if image_url_5 else ''

                image_url_6 = zoom_image_urls[5] if len(zoom_image_urls) >= 6 else ''
                file_name_6 = unique_id + '_6.png' if image_url_6 else ''
                    
                uom = []
                if product_name:
                    regex_pattern = r'\b((?:\d+x\d+(?:,\d+)?|\d+(?:,\d+)?)(?:[.,]\d+)?)\s*(ml|db|g|kg|l)\b'
                    match = re.findall(regex_pattern, product_name, re.IGNORECASE)
                    if match:
                        uom.extend(match)
                if uom:
                    grammage_quantity = uom[-1][0].strip()
                    grammage_unit = uom[-1][1].strip()
                else:
                    grammage_quantity = "1"
                    grammage_unit = "db"

                

               
                competitor_name = "fressnapf"
                store_name = ''
                store_addressline1 = ''
                store_addressline2 = ''
                store_suburb = ''
                store_state = ''
                store_postcode = ''
                store_addressid = ''
                extraction_date = ''
                brand_type = ''
                drained_weight = ''
                promotion_type = ''
                price_was = ''
                promotion_price = ''
                percentage_discount = ''
                instructions = ''
                preparationinstructions = ''
                multi_buy_item_count = ''
                multi_buy_items_price_total = ''
                package_sizeof_sellingprice = ''
                per_unit_sizedescription = ''
                variants = ''
                instructionforuse = ''
                age_of_the_product = ''
                storage_instructions = ''
                country_of_origin = ''
                allergens = ''
                nutritions = ''
                vitamins = ''
                labelling = ''
                grade = ''
                region = ''
                flavour = ''
                nutritional_information = ''
                receipies = ''
                processed_food = ''
                barcode = ''
                frozen = ''
                chilled = ''
                cooking_part = ''
                handmade = ''
                max_heating_temperature = ''
                organic_type = ''
                label_information = ''
                dimensions = ''
                special_nutrition_purpose = ''
                warranty = ''
                feeding_recommendation = ''
                model_number = ''
                usp = ''
                dosage_recommendation = ''
                food_preservation = ''
                competitor_product_key = ''
                fit_guide = ''
                occasion = ''
                material_composition = ''
                style = ''
                heel_type = ''
                heel_height = ''
                upc = ''
                features = ''
                dietary_lifestyle = ''
                importer_address = ''
                vinification_details = ''
                recycling_information = ''
                return_address = ''
                distributor_address = ''
                beer_deg = ''
                netweight = ''
                netcontent = ''
                care_instructions = ''
                manufacturer_address = ''
                alchol_by_volume = ''
                ingredients = ''
                random_weight_flag = ''
                promo_limit = ''
                multibuy_items_pricesingle = ''
                perfect_match = ''
                servings_per_pack = ''
                suitable_for = ''
                standard_drinks = ''
                environmental = ''
                grape_variety = ''
                price_was = ''
                promotion_price = ''
                warning = ''
                retail_limit = ''



                item = {}
                item['unique_id'] = unique_id
                item['competitor_name'] = competitor_name
                item['store_name'] = store_name
                item['store_addressline1']  = store_addressline1
                item['store_addressline2'] = store_addressline2
                item['store_suburb'] = store_suburb
                item['store_state'] = store_state
                item['store_postcode'] = store_postcode
                item['store_addressid'] = store_addressid
                item['extraction_date'] = extraction_date
                item['product_name'] = product_name
                item['brand'] = brand
                item['brand_type'] = brand_type
                item['grammage_quantity'] = grammage_quantity
                item['grammage_unit'] = grammage_unit
                item['drained_weight'] = drained_weight
                item['producthierarchy_level1'] = product_hierarchy_level1
                item['producthierarchy_level2'] = product_hierarchy_level2
                item['producthierarchy_level3'] = product_hierarchy_level3
                item['producthierarchy_level4'] = product_hierarchy_level4
                item['producthierarchy_level5'] = product_hierarchy_level5
                item['producthierarchy_level6'] = product_hierarchy_level6
                item['producthierarchy_level7'] = product_hierarchy_level7
                item['regular_price'] = regular_price
                item['selling_price'] = selling_price
                item['price_was'] = price_was
                item['promotion_price'] = promotion_price
                item['promotion_valid_from'] = price_valid_from
                item['promotion_valid_upto'] = promotion_valid_upto
                item['promotion_type'] = promotion_type
                item['percentage_discount'] = percentage_discount
                item['promotion_description'] = promotion_description
                item['package_sizeof_sellingprice'] = package_sizeof_sellingprice
                item['per_unit_sizedescription'] = per_unit_sizedescription
                item['price_valid_from'] = price_valid_from
                item['price_per_unit'] = price_per_unit
                item['multi_buy_item_count'] = multi_buy_item_count
                item['multi_buy_items_price_total'] = multi_buy_items_price_total
                item['currency'] = currency
                item['breadcrumb'] = breadcrumbs
                item['pdp_url'] = url
                item['variants'] = variants
                item['product_description'] = product_description
                item['instructions'] = instructions
                item['storage_instructions'] = storage_instructions
                item['preparationinstructions'] = preparationinstructions
                item['instructionforuse'] = instructionforuse
                item['country_of_origin'] = country_of_origin
                item['allergens'] = allergens
                item['age_of_the_product'] = age_of_the_product
                item['age_recommendations'] = age_recommendations
                item['flavour'] = flavour
                item['nutritions'] = nutritions
                item['nutritional_information'] = nutritional_information
                item['vitamins'] = vitamins
                item['labelling'] = labelling
                item['grade'] = grade
                item['region'] = region
                item['packaging'] = packaging
                item['receipies'] = receipies
                item['processed_food'] = processed_food
                item['barcode'] = barcode
                item['frozen'] = frozen
                item['chilled'] = chilled
                item['organictype'] = organic_type
                item['cooking_part'] = cooking_part
                item['handmade'] = handmade
                item['max_heating_temperature'] = max_heating_temperature
                item['special_information'] = special_information
                item['label_information'] = label_information
                item['dimensions'] = dimensions
                item['special_nutrition_purpose'] = special_nutrition_purpose
                item['feeding_recommendation'] = feeding_recommendation
                item['warranty'] = warranty
                item['color'] = color
                item['model_number'] = model_number
                item['material'] = material
                item['usp'] = usp
                item['dosage_recommendation'] = dosage_recommendation
                item['tasting_note'] = tasting_note
                item['food_preservation'] = food_preservation
                item['size'] = size
                item['rating'] = rating
                item['review'] = review
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
                item['competitor_product_key'] = competitor_product_key
                item['fit_guide'] = fit_guide
                item['occasion'] = occasion
                item['material_composition'] = material_composition
                item['style'] = style
                item['care_instructions'] = care_instructions
                item['heel_type'] = heel_type
                item['heel_height'] = heel_height
                item['upc'] = upc
                item['features'] = features
                item['dietary_lifestyle'] = dietary_lifestyle
                item['manufacturer_address'] = manufacturer_address
                item['importer_address'] = importer_address
                item['distributor_address'] = distributor_address
                item['vinification_details'] = vinification_details
                item['recycling_information'] = recycling_information
                item['return_address'] = return_address
                item['alchol_by_volume'] = alchol_by_volume
                item['beer_deg'] = beer_deg
                item['netcontent'] = netcontent
                item['netweight'] = netweight
                item['site_shown_uom'] = product_name
                item['ingredients'] = ingredients
                item['random_weight_flag'] = random_weight_flag
                item['instock'] = instock
                item['promo_limit'] = promo_limit
                item['product_unique_key'] = product_unique_key
                item['multibuy_items_pricesingle'] = multibuy_items_pricesingle
                item['perfect_match'] = perfect_match
                item['servings_per_pack'] = servings_per_pack
                item['warning'] = warning
                item['suitable_for'] = suitable_for
                item['standard_drinks'] = standard_drinks
                item['environmental'] = environmental
                item['grape_variety'] = grape_variety
                item['retail_limit'] = retail_limit                        

                logging.info(item)
                try:
                    self.parsed_collection.insert_one(item)
                    logging.info("item information saved successfully!")
                except Exception as e:
                    logging.error(f"Failed to save item information: {str(e)}")

            else:
                logging.error(f"Failed to fetch URL: {url}. Status code: {response.status_code}")
        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed: {str(e)}")


    def parse_urls(self, fixed_delay=5):
        while self.collection.count_documents({}) > 0:
            product_doc = self.collection.find_one_and_delete({})
            product_url = product_doc["url"]
            self.parse_product_page(product_url)
            time.sleep(fixed_delay)

if __name__ == "__main__":
    parser = fressnapfParser()
    parser.parse_urls()