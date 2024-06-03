from collections import OrderedDict
import requests
from parsel import Selector
from pymongo import MongoClient
import logging
import re
import json
from settings import get_headers
from pipeline import product_details_collection,get_mongo_collection
import time


class fressnapfParser:
    def __init__(self):
        self.collection = get_mongo_collection("fressnapf_site", "product_urls")
        self.parsed_collection = product_details_collection("fressnapf_site","product_details")

        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

        self.request_count = 0
        self.max_requests = 6000


    def parse_product_page(self, url):
        headers = get_headers()
        timeout = (5, 10)
        try:
            response = requests.get(url, headers=headers,timeout=timeout)
            self.request_count += 1
            
            if response.status_code == 200:
                selector = Selector(response.text)

                json_ld_data = selector.xpath("//script[@type ='application/ld+json']/text()").get()
                data = json.loads(json_ld_data)
                product_data = next(item for item in data if item.get('@type') == 'Product')

                unique_id = product_data["sku"]
                product_unique_key = unique_id + 'P'
                product_name = product_data["name"]
                if product_data["brand"] is not None:
                    brand = product_data["brand"]["name"]
                else:
                    brand = ''
                Currency = product_data["offers"]["priceCurrency"]
                selling_price = product_data["offers"]["price"] 

                instock = product_data["offers"]["availability"]
                if 'instock' in instock.lower():
                    instock = True
                else:
                    instock = False


                breadcrumbs = selector.xpath("//nav[@class ='breadcrumbs']/ul/li//span/text()").getall()
                if breadcrumbs:
                    breadcrumbs.insert(0, "Heimat")                  
                    product_hierarchy_level1 = breadcrumbs[0]
                    product_hierarchy_level2 = breadcrumbs[1] if len(breadcrumbs) >= 2 else ''
                    product_hierarchy_level3 = breadcrumbs[2] if len(breadcrumbs) >= 3 else ''
                    product_hierarchy_level4 = breadcrumbs[3] if len(breadcrumbs) >= 4 else ''
                    product_hierarchy_level5 = breadcrumbs[4] if len(breadcrumbs) >= 5 else ''
                    product_hierarchy_level6 = breadcrumbs[5] if len(breadcrumbs) >= 6 else ''
                    product_hierarchy_level7 = breadcrumbs[6] if len(breadcrumbs) >= 7 else ''                   
                    breadcrumb = " > ".join(breadcrumbs)


                price_data = selector.xpath("//div[@class ='pricing atcf-action-pricing pricing--large']//span[@class ='p-former-price']/span/text()").get()
                regular_price = float(price_data.replace("\xa0", " ").replace("€", "").replace(",", ".")) if price_data else ''


                promotion_description = ""
                if selector.xpath("//div[@class='product-badges']/span[@class='tag pb-tag tag--discount']/span/text()").get():
                    promotion_description = selector.xpath("//div[@class='product-badges']/span[@class='tag pb-tag tag--discount']/span/text()").get()
                percentage_discount = promotion_description.strip('%-')

                special_nutrition_purpose = selector.xpath("//div[@class='accordion-item'][h2/span[text()='Besonderer Ernährungszweck']]//div/text()").get()
                if special_nutrition_purpose is None:
                    special_nutrition_purpose = ''
                

                feeding_recommendation_paragraph = selector.xpath("//div[@class='feeding-suggestion']//text()").getall()
                feeding_recommendation_paragraph = ' '.join(feeding_recommendation_paragraph).strip().replace('\n', ' ').replace('\xa0', ' ')
                table_data = selector.xpath("//div[@class='product-detail-table']/table//text()").getall()
                table_data = [item.strip() for item in table_data if item.strip()]
                table_text = ', '.join(table_data) if table_data else ''
                feeding_data = {
                    "feeding_suggestion": feeding_recommendation_paragraph,
                    "table_data": table_text
                }


                ingredients_data = selector.xpath("//div[h2/span[contains(text(), 'Inhaltsstoffe')]]/div[@class='accordion-item-content']//text()").getall()
                cleaned_ingredients_data = [line.strip() for line in ingredients_data if line.strip()]
                ingredients = ' '.join(cleaned_ingredients_data) 


                distributor_address = selector.xpath("//table[@class='pd-documents-table']//tbody//tr[td/strong='Inverkehrbringer']/td[2]/text()").get()


                product_variant_labels = selector.xpath("//div[@class ='product-variant-slide']/div[@class ='pvs-title']/text()").getall()
                unique_product_variants = set(product_variant_labels)
                variants = [label.strip() for label in unique_product_variants]
                variants_dict = {'variant': variants}


                product_details_data = selector.xpath("//div[@id='productDetails']//div[@class='accordion-item-content']//text()").getall()
                cleaned_product_details_data = [line.strip() for line in product_details_data if line.strip()]
                description = ' '.join(cleaned_product_details_data)


                price_per_unit = None
                price_per_unit_text = selector.xpath("//div[@class ='pricing atcf-action-pricing pricing--large']/div[@class ='p-per-unit p-regular-price']/text()").get()
                if not price_per_unit_text:
                    price_per_unit_text = selector.xpath("//div[@class ='pricing atcf-action-pricing pricing--large']//div[@class ='p-per-unit p-regular-price p-with-savings']/text()").get()
                if price_per_unit_text:
                    price_parts = price_per_unit_text.split()              
                    if len(price_parts) > 1:
                        price_numeric = price_parts[1]
                        price_numeric = price_numeric.replace(',', '.')
                        price_per_unit = "€" + price_numeric
                        price_per_unit = price_per_unit.strip(')')
                else:
                    price_per_unit = " "
                price_per_unit = price_per_unit if price_per_unit is not None else " "



                title_text = selector.xpath("//div[@class ='heading pos-title h4']/text()").get()
                grammage_quantity = " "
                grammage_unit = " "
                if title_text:
                    numerical_value_match = re.search(r'\b(\d+x\d+(?:,\d+)?|\d+(?:,\d+)?)\s*([mllkg]+)', title_text)
                    if numerical_value_match:
                        grammage_quantity = numerical_value_match.group(1)
                        grammage_unit = numerical_value_match.group(2) 


            
                api_url = f"https://api.bazaarvoice.com/data/display/0.2alpha/product/summary?PassKey=cauYrgaqJLZ4Xi1fzpXf3L4Jz1EWyxzsTrXg4XwY3czn4&productid={unique_id}&contentType=reviews,questions&reviewDistribution=primaryRating,recommended&rev=0&contentlocale=nl*,en*,fr*,de*,it*,pl*,de_AT"
                review_rating_response = requests.get(api_url, headers=headers)
                if review_rating_response.status_code == 200:
                    email_data = review_rating_response.json()
                    review = email_data["reviewSummary"]["numReviews"] if 'reviewSummary' in email_data else None
                    rating = email_data["reviewSummary"]["primaryRating"]["average"] if 'reviewSummary' in email_data else None
                    if rating is not None:
                        rating = f"{float(rating):.2f}"
                else:
                    review = ''
                    rating = ''


                image_urls = selector.xpath("//div[@class ='zoom-image g-image']/a/@href").extract()
                unique_urls = list(OrderedDict.fromkeys(image_urls))
                unique_urls = unique_urls[:6]

                image_url_1 = unique_urls[0] if len(unique_urls) >= 1 else ''
                file_name_1 = unique_id + '_1.png' if image_url_1 else ''

                image_url_2 = unique_urls[1] if len(unique_urls) >= 2 else ''
                file_name_2 = unique_id + '_2.png' if image_url_2 else ''

                image_url_3 = unique_urls[2] if len(unique_urls) >= 3 else ''
                file_name_3 = unique_id + '_3.png' if image_url_3 else ''

                image_url_4 = unique_urls[3] if len(unique_urls) >= 4 else ''
                file_name_4 = unique_id + '_4.png' if image_url_4 else ''

                image_url_5 = unique_urls[4] if len(unique_urls) >= 5 else ''
                file_name_5 = unique_id + '_5.png' if image_url_5 else ''

                image_url_6 = unique_urls[5] if len(unique_urls) >= 6 else ''
                file_name_6 = unique_id + '_6.png' if image_url_6 else ''



                retail_limit = ''
                retail_limit_text = selector.xpath("//div[@class ='c-option-label']/div/text()").getall()
                numeric_elements = [element for element in retail_limit_text if 'x' in element]
                numeric_numbers = []
                for x in numeric_elements:
                    numbers = re.findall(r'\d+', x)
                    if numbers:
                        numeric_numbers.append(int(numbers[0]))
                if numeric_numbers:
                    highest_number = max(numeric_numbers)
                    retail_limit = str(highest_number) + "x"


                netcontent_data = selector.xpath("//div[@id='productDetails']//p[contains(text(), 'Inahlt')]//text()").get()
                netcontent = ''
                if netcontent_data:
                    match = re.search(r'Inahlt:\s*([^<]+)', netcontent_data)
                    if match:
                        netcontent = match.group(1).strip()



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
                promotion_valid_from = ''
                promotion_valid_upto = ''
                promotion_type = ''
                instructions = ''
                storage_instructions = ''
                preparationinstructions = ''
                multi_buy_item_count = ''
                multi_buy_items_price_total = ''
                package_sizeof_sellingprice = ''
                per_unit_sizedescription = ''
                price_valid_from = ''
                instructionforuse = ''
                country_of_origin = ''
                allergens = ''
                age_of_the_product = ''
                age_recommendations = ''
                flavour = ''
                nutritions = ''
                nutritional_information = ''
                vitamins = ''
                labelling = ''
                grade = ''
                region = ''
                packaging = ''
                receipies = ''
                processed_food = ''
                barcode = ''
                frozen = ''
                chilled = ''
                organictype = ''
                cooking_part = ''
                handmade = ''
                max_heating_temperature = ''
                special_information = ''
                label_information = ''
                dimensions = ''
                warranty = ''
                color = ''
                model_number = ''
                material = ''
                usp = ''
                dosage_recommendation = ''
                tasting_note = ''
                food_preservation = ''
                size = ''
                competitor_product_key = ''
                fit_guide = ''
                occasion = ''
                material_composition = ''
                style = ''
                care_instructions = ''
                heel_type = ''
                heel_height = ''
                upc = ''
                features = ''
                dietary_lifestyle = ''
                manufacturer_address = ''
                importer_address = ''
                vinification_details = ''
                recycling_information = ''
                return_address = ''
                alchol_by_volume = ''
                beer_deg = ''
                netweight = ''
                random_weight_flag = ''
                promo_limit = ''
                multibuy_items_pricesingle = ''
                perfect_match = ''
                servings_per_pack = ''
                warning = ''
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
                item['currency'] = Currency
                item['breadcrumb'] = breadcrumb
                item['pdp_url'] = url
                item['variants'] = variants_dict
                item['product_description'] = description
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
                item['organictype'] = organictype
                item['cooking_part'] = cooking_part
                item['handmade'] = handmade
                item['max_heating_temperature'] = max_heating_temperature
                item['special_information'] = special_information
                item['label_information'] = label_information
                item['dimensions'] = dimensions
                item['special_nutrition_purpose'] = special_nutrition_purpose
                item['feeding_recommendation'] = feeding_data
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