import requests
import logging
import re
import json
from settings import parser_api_headers
from pipeline import MongoPipeline
import time

class FressnapfParser:
    def __init__(self):
        self.mongo_pipeline = MongoPipeline()
        self.collection = self.mongo_pipeline.db["product_urls"]
        self.parsed_collection = self.mongo_pipeline.db['product_details']

        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    def parse_product_page(self, url):
        timeout = (5, 10)
        try:
            match = re.search(r'\d+$', url)
            if match:
                unique_id = match.group(0)
            else:
                unique_id = ''

            api_url = f'https://www.migros.ch/product-display/public/v2/product-detail?storeType=ONLINE&warehouseId=2&region=national&migrosIds={unique_id}'
            response = requests.get(api_url, headers=parser_api_headers, timeout=timeout)

            if response.status_code == 200:
                product_data = response.json()
                product_info = product_data[0]

                product_name = product_info.get('title', '')
    
                brand = product_info.get('brand', '')
                
                offer_info = product_info.get('offer', {})
                price_per_unit = offer_info.get('quantityPrice', '')
                site_shown_uom = offer_info.get('quantity', '') #clean take the grammagy quntity and unit
                # grammage_quantity = grammage_quantity.replace(r'\u2009', ' ')
                selling_price = offer_info.get('price', {}).get('formattedPrice', '')
                
                badges = offer_info.get('badges', [])
                if badges:
                    promotion_type = badges[0].get('type','')
                    promotion_description = badges[0].get('description','')
                else:
                    promotion_type = ''
                    promotion_description = ''
                    

                breadcrumbs_data = product_info.get('breadcrumb', [])
                breadcrumbs = [breadcrumb.get('name', '') for breadcrumb in breadcrumbs_data]
                if breadcrumbs:
                    breadcrumbs.insert(0, "Startseite")
                    product_hierarchy_level1 = breadcrumbs[0]
                    product_hierarchy_level2 = breadcrumbs[1] if len(breadcrumbs) >= 2 else ''
                    product_hierarchy_level3 = breadcrumbs[2] if len(breadcrumbs) >= 3 else ''
                    product_hierarchy_level4 = breadcrumbs[3] if len(breadcrumbs) >= 4 else ''
                    product_hierarchy_level5 = breadcrumbs[4] if len(breadcrumbs) >= 5 else ''
                    product_hierarchy_level6 = breadcrumbs[5] if len(breadcrumbs) >= 6 else ''
                    product_hierarchy_level7 = breadcrumbs[6] if len(breadcrumbs) >= 7 else ''                   
                    breadcrumb = " > ".join(breadcrumbs)


                item = {}
                item['unique_id'] = unique_id
                # item['competitor_name'] = competitor_name
                # item['store_name'] = store_name
                # item['store_addressline1']  = store_addressline1
                # item['store_addressline2'] = store_addressline2
                # item['store_suburb'] = store_suburb
                # item['store_state'] = store_state
                # item['store_postcode'] = store_postcode
                # item['store_addressid'] = store_addressid
                # item['extraction_date'] = extraction_date
                item['product_name'] = product_name
                item['brand'] = brand
                # item['brand_type'] = brand_type
                # item['grammage_quantity'] = grammage_quantity
                # item['grammage_unit'] = grammage_unit
                # item['drained_weight'] = drained_weight
                item['producthierarchy_level1'] = product_hierarchy_level1
                item['producthierarchy_level2'] = product_hierarchy_level2
                item['producthierarchy_level3'] = product_hierarchy_level3
                item['producthierarchy_level4'] = product_hierarchy_level4
                item['producthierarchy_level5'] = product_hierarchy_level5
                item['producthierarchy_level6'] = product_hierarchy_level6
                item['producthierarchy_level7'] = product_hierarchy_level7
                # item['regular_price'] = regular_price
                item['selling_price'] = selling_price
                # item['price_was'] = price_was
                # item['promotion_price'] = promotion_price
                # item['promotion_valid_from'] = price_valid_from
                # item['promotion_valid_upto'] = promotion_valid_upto
                item['promotion_type'] = promotion_type
                # item['percentage_discount'] = percentage_discount
                item['promotion_description'] = promotion_description
                # item['package_sizeof_sellingprice'] = package_sizeof_sellingprice
                # item['per_unit_sizedescription'] = per_unit_sizedescription
                # item['price_valid_from'] = price_valid_from
                item['price_per_unit'] = price_per_unit
                # item['multi_buy_item_count'] = multi_buy_item_count
                # item['multi_buy_items_price_total'] = multi_buy_items_price_total
                # item['currency'] = currency
                item['breadcrumb'] = breadcrumb
                item['pdp_url'] = url
                # item['variants'] = variants
                # item['product_description'] = product_description
                # item['instructions'] = instructions
                # item['storage_instructions'] = storage_instructions
                # item['preparationinstructions'] = preparationinstructions
                # item['instructionforuse'] = instructionforuse
                # item['country_of_origin'] = country_of_origin
                # item['allergens'] = allergens
                # item['age_of_the_product'] = age_of_the_product
                # item['age_recommendations'] = age_recommendations
                # item['flavour'] = flavour
                # item['nutritions'] = nutritions
                # item['nutritional_information'] = nutritional_information
                # item['vitamins'] = vitamins
                # item['labelling'] = labelling
                # item['grade'] = grade
                # item['region'] = region
                # item['packaging'] = packaging
                # item['receipies'] = receipies
                # item['processed_food'] = processed_food
                # item['barcode'] = barcode
                # item['frozen'] = frozen
                # item['chilled'] = chilled
                # item['organictype'] = organic_type
                # item['cooking_part'] = cooking_part
                # item['handmade'] = handmade
                # item['max_heating_temperature'] = max_heating_temperature
                # item['special_information'] = special_information
                # item['label_information'] = label_information
                # item['dimensions'] = dimensions
                # item['special_nutrition_purpose'] = special_nutrition_purpose
                # item['feeding_recommendation'] = feeding_recommendation
                # item['warranty'] = warranty
                # item['color'] = color
                # item['model_number'] = model_number
                # item['material'] = material
                # item['usp'] = usp
                # item['dosage_recommendation'] = dosage_recommendation
                # item['tasting_note'] = tasting_note
                # item['food_preservation'] = food_preservation
                # item['size'] = size
                # item['rating'] = rating
                # item['review'] = review
                # item['file_name_1'] = file_name_1
                # item['image_url_1'] = image_url_1
                # item['file_name_2'] = file_name_2
                # item['image_url_2'] = image_url_2
                # item['file_name_3'] = file_name_3
                # item['image_url_3'] = image_url_3
                # item['file_name_4'] = file_name_4
                # item['image_url_4'] = image_url_4
                # item['file_name_5'] = file_name_5
                # item['image_url_5'] = image_url_5
                # item['file_name_6'] = file_name_6
                # item['image_url_6'] = image_url_6
                # item['competitor_product_key'] = competitor_product_key
                # item['fit_guide'] = fit_guide
                # item['occasion'] = occasion
                # item['material_composition'] = material_composition
                # item['style'] = style
                # item['care_instructions'] = care_instructions
                # item['heel_type'] = heel_type
                # item['heel_height'] = heel_height
                # item['upc'] = upc
                # item['features'] = features
                # item['dietary_lifestyle'] = dietary_lifestyle
                # item['manufacturer_address'] = manufacturer_address
                # item['importer_address'] = importer_address
                # item['distributor_address'] = distributor_address
                # item['vinification_details'] = vinification_details
                # item['recycling_information'] = recycling_information
                # item['return_address'] = return_address
                # item['alchol_by_volume'] = alchol_by_volume
                # item['beer_deg'] = beer_deg
                # item['netcontent'] = netcontent
                # item['netweight'] = netweight
                item['site_shown_uom'] = site_shown_uom
                # item['ingredients'] = ingredients
                # item['random_weight_flag'] = random_weight_flag
                # item['instock'] = instock
                # item['promo_limit'] = promo_limit
                # item['product_unique_key'] = product_unique_key
                # item['multibuy_items_pricesingle'] = multibuy_items_pricesingle
                # item['perfect_match'] = perfect_match
                # item['servings_per_pack'] = servings_per_pack
                # item['warning'] = warning
                # item['suitable_for'] = suitable_for
                # item['standard_drinks'] = standard_drinks
                # item['environmental'] = environmental
                # item['grape_variety'] = grape_variety
                # item['retail_limit'] = retail_limit                        


                logging.info(item)
                try:
                    self.parsed_collection.insert_one(item)
                    logging.info("Item information saved successfully!")
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
    parser = FressnapfParser()
    parser.parse_urls()
