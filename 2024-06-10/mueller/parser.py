import requests
from parsel import Selector
from pymongo import MongoClient
import logging
import re
from json import JSONDecodeError
import json
from settings import headers
from pipeline import product_details_collection,get_mongo_collection
import time


class fressnapfParser:
    def __init__(self):
        self.mongo_client = MongoClient('mongodb://localhost:27017/')
        self.db = self.mongo_client['mueller_site']
        self.collection = self.db['product_urls']
        self.parsed_collection = self.db['product_details']
        # self.collection = get_mongo_collection("fressnapf_site", "product_urls")
        # self.parsed_collection = product_details_collection("fressnapf_site","product_details")
        self.parsed_collection.create_index([('pdp_url', 1)], unique=True)

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


                script_elements = selector.xpath('//script[contains(text(), "dataLayer")]/text()').getall()
                script_element = script_elements[0]
                match = re.search(r'dataLayer = (\[.*?\]);', script_element, re.DOTALL)
                if match:
                    json_data = match.group(1)
                    data = json.loads(json_data)
                    brand = data[0]['ecommerce']['detail']['products'][0]['brand']
                    currency = data[0]['ecommerce']['currencyCode']
                    product_name = data[0]['ecommerce']['detail']['products'][0]['name']
                    unique_id = data[0]['ecommerce']['detail']['products'][0]['id']
                    product_unique_key = unique_id + 'P'

                pdp_url = f"{url}?itemId={unique_id}"

                breadcrumbs_data = selector.xpath("//ul[@class ='mu-breadcrumbs']/li//span/text()").getall()
                breadcrumbs = [breadcrumb.strip() for breadcrumb in breadcrumbs_data]
                if breadcrumbs:                 
                    product_hierarchy_level1 = breadcrumbs[0]
                    product_hierarchy_level2 = breadcrumbs[1] if len(breadcrumbs) >= 2 else ''
                    product_hierarchy_level3 = breadcrumbs[2] if len(breadcrumbs) >= 3 else ''
                    product_hierarchy_level4 = breadcrumbs[3] if len(breadcrumbs) >= 4 else ''
                    product_hierarchy_level5 = breadcrumbs[4] if len(breadcrumbs) >= 5 else ''
                    product_hierarchy_level6 = breadcrumbs[5] if len(breadcrumbs) >= 6 else ''
                    product_hierarchy_level7 = breadcrumbs[6] if len(breadcrumbs) >= 7 else ''                   
                    breadcrumb = " > ".join(breadcrumbs)


                manufacturer_address = selector.xpath("//td[contains(text(), 'Herstelleradresse')]/following-sibling::td/text()").get()
                manufacturer_address = manufacturer_address.strip().replace('\n', ' ') if manufacturer_address else ''


                ingredients = selector.xpath("//td[contains(text(), 'Inhaltsstoffe')]/following-sibling::td/text()").get()
                if not ingredients:
                    ingredients = selector.xpath("//td[contains(text(), 'Zutaten')]/following-sibling::td/text()").get()
                ingredients = ingredients.strip().replace('\n', ' ') if ingredients else ''


                flavour = selector.xpath("//td[contains(text(),'Geschmacksrichtung')]/following-sibling::td/text()").get()
                flavour = flavour.strip() if flavour else ''


                color = selector.xpath("//td[contains(text(), 'Farbe')]/following-sibling::td/text()").get()
                color = color.strip() if color else ''


                packaging = selector.xpath("//td[contains(text(),'Hundegröße')]/following-sibling::td/text()").get()
                packaging =  packaging.strip() if packaging else ''


                storage_instructions = selector.xpath("//td[contains(text(),'Lagerhinweis')]/following-sibling::td/text()").get()
                storage_instructions = storage_instructions.strip() if storage_instructions else ''


                product_details_data = selector.xpath("//div[@class='mu-product-description__content']//text()").getall()
                cleaned_product_details_data = [line.strip().replace('\n', ' ') for line in product_details_data if line.strip()]
                product_description = ' '.join(cleaned_product_details_data)


                warning_data = selector.xpath("//div[@class ='mu-product-description__notice']//text()").getall()
                cleaned_product_warning_data = [line.strip() for line in warning_data if line.strip()]
                warning = ' '.join(cleaned_product_warning_data)

               
                allergens = selector.xpath("//td[contains(text(),'Allergenhinweis')]/following-sibling::td/text()").get()
                allergens = allergens.strip() if allergens else ''


                country_of_origin = selector.xpath("//td[contains(text(),'Herkunftsland')]/following-sibling::td/text()").get()
                country_of_origin = country_of_origin.strip() if country_of_origin else ''


                model_number = selector.xpath("//td[contains(text(),'Modellnummer')]/following-sibling::td/text()").get()
                model_number = model_number.strip() if model_number else ''


                percentage_discount = selector.xpath("//div[@class='mu-product-price__percentage-saving']/strong/text()").get()
                percentage_discount = percentage_discount.replace('%', '').strip() if percentage_discount else ''
  

                promotion_description_list = selector.xpath("//div[@class='mu-product-price__percentage-saving']//text()").getall()
                promotion_description_list = [text.strip() for text in promotion_description_list if text.strip()]
                promotion_description = ' '.join(promotion_description_list)

                price_str = selector.xpath("//span[@class='mu-product-price__price mu-product-price__price--original']/text()").get()
                regular_price = price_str.replace('€', '').replace(',', '.').strip() if price_str else ''


                care_instructions = selector.xpath("//td[contains(text(),'Pflegehinweis')]/following-sibling::td/text()").get()
                care_instructions = care_instructions.strip() if care_instructions else ''

                
                material = selector.xpath("//td[contains(text(),'Material')]/following-sibling::td/text()").get()
                material = material.strip() if material else ''

                size =  selector.xpath("//td[contains(text(),'Größe')]/following-sibling::td/text()").get()
                size = size.strip() if size else ''

                feeding_recommendation = selector.xpath("//td[contains(text(),'Fütterungsempfehlung')]/following-sibling::td/text()").get()
                feeding_recommendation = feeding_recommendation.strip() if feeding_recommendation else ''

                tasting_note = selector.xpath("//td[contains(text(),'Geschmack')]/following-sibling::td/text()").get()
                tasting_note = tasting_note.strip() if tasting_note else ''

                alchol_by_volume = selector.xpath("//td[contains(text(),'Lebensmittel Alkoholgehalt')]/following-sibling::td/text()").get()
                alchol_by_volume =  alchol_by_volume.strip() if alchol_by_volume else ''


                organic_type_data = selector.xpath("//td[contains(text(),'Bio')]/following-sibling::td/text()").get()
                organic_type = organic_type_data.strip() if organic_type_data else ''
                if organic_type and 'ja' in organic_type.lower():
                    organic_type = 'Organic'
                else:
                    organic_type = 'Non-Organic'


                price_per_unit = selector.xpath("//div[@class='mu-product-price__additional-info']/text()").get()
                if price_per_unit:
                    if any(char.isdigit() for char in price_per_unit):
                        price_per_unit = price_per_unit.strip().split('\xa0')[0].replace(',', '.')
                    else:
                        price_per_unit = ''
                else:
                    price_per_unit = ''


                script_text = selector.xpath("//script[contains(text(),'window.__APP_STATE')]").get()
                matches_retail_limit = re.search(r'availableQuantitiesForPurchase\\":\[(.*?)\]', script_text)
                if matches_retail_limit:
                    numbers = [int(x.strip()) for x in matches_retail_limit.group(1).split(",")]
                    retail_limit = max(numbers) if any(numbers) else ''
                else:
                    retail_limit = ''

                matches_grammege = re.search(r"name:\s+'Inhalt',\s+value:\s+'([^']*)'", script_text)
                if matches_grammege:
                    inhalt_value = matches_grammege.group(1)
                    quantity = re.search(r'(\d+)\s+(\S+)', inhalt_value)
                    if quantity:
                        grammage_quantity = int(quantity.group(1))
                        grammage_unit = quantity.group(2)
                        site_shown_uom = f"{grammage_quantity} {grammage_unit}"


                product_data = None
                start = script_text.find('{', script_text.find('product: \''))
                end = script_text.find('\'', start)
                json_str = script_text[start:end]

                json_str = json_str.replace("'", '"')
                json_str = json_str.replace('\\"', '"')
                try:
                    product_data = json.loads(json_str)
                except json.decoder.JSONDecodeError as e:
                    print("JSONDecodeError:", e)
                
                if product_data is not None:
                    image_urls = [
                        asset['image']['zoom']
                        for asset in product_data['assets']
                        if 'image' in asset and 'zoom' in asset['image']
                    ]
                else:
                    image_urls = []

                image_url_1 = image_urls[0] if len(image_urls) > 0 else ''
                file_name_1 = unique_id + '_1.png' if image_url_1 else ''

                image_url_2 = image_urls[1] if len(image_urls) > 1 else ''
                file_name_2 = unique_id + '_2.png' if image_url_2 else ''

                image_url_3 = image_urls[2] if len(image_urls) > 2 else ''
                file_name_3 = unique_id + '_3.png' if image_url_3 else ''

                image_url_4 = image_urls[3] if len(image_urls) > 3 else ''
                file_name_4 = unique_id + '_4.png' if image_url_4 else ''

                image_url_5 = image_urls[4] if len(image_urls) > 4 else ''
                file_name_5 = unique_id + '_5.png' if image_url_5 else ''

                image_url_6 = image_urls[5] if len(image_urls) > 5 else ''
                file_name_6 = unique_id + '_6.png' if image_url_6 else ''


                
                variants = {}  
                try:
                    if product_data is not None: 
                        variations = product_data.get('variations', []) 
                        for variation in variations:
                            display_name = variation.get('displayName', '')  
                            if display_name in ['Farbe', 'Größenspanne']:
                                variants[display_name] = [item['value'] for item in variation.get('variations', [])]
                    else:
                        print("product data not avialable.")
                except JSONDecodeError as e:
                    print("JSONDecodeError:", e)


                api_url = f"https://api.bazaarvoice.com/data/display/0.2alpha/product/summary?PassKey=caq5uCLiy6gWz7GqQ81mhGOWABJAsK961yPzzEfe0S9Ng&productid={unique_id}&contentType=reviews,questions&reviewDistribution=primaryRating,recommended&rev=0&contentlocale=de*,de_AT"
                review_rating_response = requests.get(api_url, headers=headers)
                if review_rating_response.status_code == 200:
                    email_data = review_rating_response.json()
                    review = email_data["reviewSummary"]["numReviews"] if 'reviewSummary' in email_data else ''
                    rating = email_data["reviewSummary"]["primaryRating"]["average"] if 'reviewSummary' in email_data else ''
                    if review == 0:
                        review = ''
                        rating = ''
                else:
                    review = ''
                    rating = ''


                availablity_json = selector.xpath("//component[@type='application/ld+json']/text()").get()
                json_datas = json.loads(availablity_json)
                availability = json_datas['offers'][0]['availability']
                if 'instock' in availability.lower():
                    instock = True
                else:
                    instock = False
                selling_price = json_datas.get('offers', [{}])[0].get('price')

                
                age_from = selector.xpath("//td[contains(text(),'Altersempfehlung ab')]/following-sibling::td/text()").get()
                age_upto = selector.xpath("//td[contains(text(),'Altersempfehlung bis')]/following-sibling::td/text()").get()
                if age_from and age_upto:
                    age_recommendations = f"{age_from.strip()} {age_upto.strip()}"
                elif age_from:
                    age_recommendations = age_from.strip()
                elif age_upto:
                    age_recommendations = age_upto.strip()
                else:
                    age_recommendations = ''


                header = selector.xpath("//div[@id ='nutrition']/table//tr[1]//text()").getall()
                header = [item.strip() for item in header if item.strip()] 

                elements = selector.xpath("//div[@id ='nutrition']/table//tr[position()>1]//text()").getall()
                elements = [item.strip() for item in elements if item.strip()]

                if len(header) >= 2:
                    header_key_part1 = header[0] 
                    header_key_part2 = header[1] 

                nutritional_information = {}
                for index, item in enumerate(elements):
                    if index % 2 == 0: 
                        current_key = f"{header_key_part1}_{item}_{header_key_part2}"
                    else: 
                        nutritional_information[current_key] = item


                manufacture_elements = selector.xpath("//td[contains(text(),'Hersteller')]/following-sibling::td/text()").extract()
                manufacture = manufacture_elements[0].strip() if manufacture_elements else ''

                effect_elements = selector.xpath("//td[contains(text(),'Effekt')]/following-sibling::td/text()").extract()
                effect = effect_elements[0].strip() if effect_elements else ''

                consistancy_elements = selector.xpath("//td[contains(text(),'Konsistenz')]/following-sibling::td/text()").extract()
                consistancy = consistancy_elements[0].strip() if consistancy_elements else ''

                skin_type_elements = selector.xpath("//td[contains(text(),'Hauttyp')]/following-sibling::td/text()").extract()
                skin_type = skin_type_elements[0].strip() if skin_type_elements else ''

                scope_of_appication_elements = selector.xpath("//td[contains(text(),'Anwendungsbereich')]/following-sibling::td/text()").extract()
                scope_of_appication = scope_of_appication_elements[0].strip() if scope_of_appication_elements else ''

                opacity_elements = selector.xpath("//td[contains(text(),'Deckkraft')]/following-sibling::td/text()").extract()
                opacity = opacity_elements[0].strip() if opacity_elements else ''

                target_group_elements = selector.xpath("//td[contains(text(),'Zielgruppe')]/following-sibling::td/text()").extract()
                target_group = target_group_elements[0].strip() if target_group_elements else ''

                application_note_elements = selector.xpath("//td[contains(text(),'Anwendungshinweis')]/following-sibling::td/text()").extract()
                application_note = application_note_elements[0].strip() if application_note_elements else ''

                dermatologically_tested_elements = selector.xpath("//td[contains(text(),'Dermatologisch getestet')]/following-sibling::td/text()").extract()
                dermatologically_tested = dermatologically_tested_elements[0].strip() if dermatologically_tested_elements else ''

                scope_of_delivery_elements = selector.xpath("//td[contains(text(),'Lieferumfang')]/following-sibling::td/text()").extract()
                scope_of_delivery = scope_of_delivery_elements[0].strip() if scope_of_delivery_elements else ''

                license_elements = selector.xpath("//td[contains(text(),'Lizenz')]/following-sibling::td/text()").extract()
                license = license_elements[0].strip() if license_elements else ''

                medium_elements = selector.xpath("//td[contains(text(),'Medium')]/following-sibling::td/text()").extract()
                medium = medium_elements[0].strip() if medium_elements else ''

                genre_elements = selector.xpath("//td[contains(text(),'Genre')]/following-sibling::td/text()").extract()
                genre = genre_elements[0].strip() if genre_elements else ''

                label_elements = selector.xpath("//td[contains(text(),'Label')]/following-sibling::td/text()").extract()
                label = label_elements[0].strip() if label_elements else ''

                special_information = {
                    "manufacture": manufacture,
                    "effect": effect,
                    "consistancy": consistancy,
                    "skin_type": skin_type,
                    "scope_of_appication": scope_of_appication,
                    "opacity": opacity,
                    "target_group": target_group,
                    "application_note": application_note,
                    "dermatologically_tested": dermatologically_tested,
                    "scope_of_delivery": scope_of_delivery,
                    "license": license,
                    "medium" : medium,
                    "genre" : genre,
                    "label" : label
                }
                special_information = {k: v for k, v in special_information.items() if v}
                special_information = ' '.join([f"{k}:{v}" for k, v in special_information.items()])

               
                competitor_name = "mueller"
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
                promotion_valid_from = ''
                promotion_valid_upto = ''
                promotion_type = ''
                instructions = ''
                preparationinstructions = ''
                multi_buy_item_count = ''
                multi_buy_items_price_total = ''
                package_sizeof_sellingprice = ''
                per_unit_sizedescription = ''
                price_valid_from = ''
                instructionforuse = ''
                age_of_the_product = ''
                nutritions = ''
                vitamins = ''
                labelling = ''
                grade = ''
                region = ''
                receipies = ''
                processed_food = ''
                barcode = ''
                frozen = ''
                chilled = ''
                cooking_part = ''
                handmade = ''
                max_heating_temperature = ''
                label_information = ''
                dimensions = ''
                special_nutrition_purpose = ''
                warranty = ''
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
                random_weight_flag = ''
                promo_limit = ''
                multibuy_items_pricesingle = ''
                perfect_match = ''
                servings_per_pack = ''
                suitable_for = ''
                standard_drinks = ''
                environmental = ''
                grape_variety = ''



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
                item['price_was'] = regular_price
                item['promotion_price'] = selling_price
                item['promotion_valid_from'] = promotion_valid_from
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
                item['breadcrumb'] = breadcrumb
                item['pdp_url'] = pdp_url
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
                item['site_shown_uom'] = site_shown_uom
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