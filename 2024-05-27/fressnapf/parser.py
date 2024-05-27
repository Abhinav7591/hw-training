import requests
from parsel import Selector
from pymongo import MongoClient
import logging
import re
import json
from pipeline import get_headers

class fressnapfParser:
    def __init__(self):
        self.mongo_client = MongoClient('mongodb://localhost:27017/')
        self.db = self.mongo_client['fressnapf']
        self.collection = self.db['product_urls']
        self.parsed_collection = self.db['product_details']
        self.parsed_collection.create_index([('pdp_url', 1)], unique=True)

        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    def parse_agent_page(self, url):
        headers = get_headers()
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            selector = Selector(response.text)

            json_ld_data = selector.xpath("//script[@type ='application/ld+json']/text()").get()
            data = json.loads(json_ld_data)
            product_data = next(item for item in data if item.get('@type') == 'Product')

            unique_id = product_data["sku"]
            product_unique_key = unique_id + 'P'
            product_name = product_data["name"]

            # for image in product_data["image"]:
            #     print(image)
            brand = product_data["brand"]["name"]
            Currency = product_data["offers"]["priceCurrency"]
            selling_price = product_data["offers"]["price"] 

            instock = product_data["offers"]["availability"]
            if 'instock' in instock.lower():
               instock = True
            else:
               instock = False


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

                 
            regular_price = selector.xpath("normalize-space(//div[@class='pricing atcf-action-pricing pricing--large']/div[@class='p-former'])").get()


            promotion_description = ""
            if selector.xpath("//div[@class='product-badges']/span[@class='tag pb-tag tag--alert-detail']/span/text()").get():
                promotion_description = selector.xpath("//div[@class='product-badges']/span[@class='tag pb-tag tag--alert-detail']/span/text()").get()
            percentage_discount = promotion_description.strip('%-')
                
                    
            feeding_recommendation_paragraph = selector.xpath("//div[@class ='feeding-suggestion']//text()").get()
            table_data = selector.xpath("//div[@class ='product-detail-table']/table//text()").getall()
            combined_data = []
            if feeding_recommendation_paragraph:
                combined_data.append(feeding_recommendation_paragraph.strip())
            else:
                combined_data.extend(item.strip() for item in table_data if item.strip())
            feeding_recommendation = ' '.join(combined_data)


            analytical_data = selector.xpath("//div//strong[text()='Analytische Bestandteile']/following-sibling::div/p/text()").get() or ''
            composition_data = selector.xpath("//div//strong[text()='Zusammensetzung']/following-sibling::div/text()").get() or ''
            additive_data = selector.xpath("//div//strong[text()='Zusatzstoffe pro kg']/following-sibling::div/text()").get() or ''
            ingredients = f"{analytical_data} {composition_data} {additive_data}"


            distributor_address = selector.xpath("//table[@class='pd-documents-table']//tbody//tr[td/strong='Inverkehrbringer']/td[2]/text()").get()


            product_variant_labels = selector.xpath("//div[@class ='product-variant-selector-item-label']/div[@class ='pvsil-title']/text()").getall()
            unique_product_variants = set(product_variant_labels) 
            variants = [label.strip() for label in unique_product_variants] 


            description = selector.xpath("//div[@id='productDetails']//div[@class='paragraph']/text()").get()
            additional_paragraph = selector.xpath("//div[@id='productDetails']//div[@class='paragraph']/p/text()").get()
            if additional_paragraph:
                description += ' ' + additional_paragraph.strip()
            description = description.strip() if description else ''



            price_per_unit_text = selector.xpath("//div[@class='p-per-unit']/text()").get()
            if price_per_unit_text:
                price_numeric = price_per_unit_text.split()[1]  
                price_numeric = price_numeric.replace(',', '.')
                price_per_unit = "€" + price_numeric



            # title_text = selector.xpath("//div[@class ='heading pos-title h4']/text()").get()
            # numerical_value_match = re.search(r'(\d+x\d+)\s*([lkg]+)', title_text)
            # if numerical_value_match:
            #     grammage_quantity = numerical_value_match.group(1)
            #     grammage_unit = numerical_value_match.group(2)
            # else:
            #     grammage_quantity = " "
            #     grammage_unit = " " 

            title_text = selector.xpath("//div[@class ='heading pos-title h4']/text()").get()
            grammage_quantity = " "
            grammage_unit = " "
            if title_text:
                numerical_value_match = re.search(r'(\d+x\d+|\d+)\s*([lkg]+)', title_text)
                if numerical_value_match:
                    grammage_quantity = numerical_value_match.group(1)
                    grammage_unit = numerical_value_match.group(2) #error


         
            api_url = f"https://api.bazaarvoice.com/data/display/0.2alpha/product/summary?PassKey=cauYrgaqJLZ4Xi1fzpXf3L4Jz1EWyxzsTrXg4XwY3czn4&productid={unique_id}&contentType=reviews,questions&reviewDistribution=primaryRating,recommended&rev=0&contentlocale=nl*,en*,fr*,de*,it*,pl*,de_AT"
            review_rating_response = requests.get(api_url, headers=headers)
            if review_rating_response.status_code == 200:
                email_data = review_rating_response.json()
                review = email_data["reviewSummary"]["numReviews"] if 'reviewSummary' in email_data else None
                rating = email_data["reviewSummary"]["primaryRating"]["average"] if 'reviewSummary' in email_data else None
            else:
                review = ''
                rating = ''



            image_urls = selector.xpath("//div[@class ='zoom-image g-image']/a/@href").extract()
            unique_urls = list(set(image_urls))
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



            product_info = {
                'unique_id' : unique_id,
                'competitor_name' : competitor_name,
                'store_name' : store_name,
                'store_addressline1' : store_addressline1,
                'store_addressline2' : store_addressline2,
                'store_suburb': store_suburb,
                'store_state' : store_state,
                'store_postcode' : store_postcode,
                'store_addressid' : store_addressid,
                'extraction_date' : extraction_date,
                'product_name' : product_name,
                'brand' : brand,
                'brand_type' : brand_type,
                'grammage_quantity': grammage_quantity,
                'grammage_unit' : grammage_unit,
                'drained_weight' : drained_weight,
                'producthierarchy_level1' : product_hierarchy_level1,
                'producthierarchy_level2': product_hierarchy_level2,
                'producthierarchy_level3' : product_hierarchy_level3,
                'producthierarchy_level4' : product_hierarchy_level4,
                'producthierarchy_level5' : product_hierarchy_level5,
                'producthierarchy_level6' : product_hierarchy_level6,
                'producthierarchy_level7' : product_hierarchy_level7,
                'regular_price' : regular_price,
                'selling_price': selling_price,
                'price_was' : regular_price,
                'promotion_price' : selling_price,
                'promotion_valid_from'
                'promotion_valid_upto'
                'promotion_type'
                'percentage_discount' : percentage_discount,
                'promotion_description' : promotion_description,
                # 'package_sizeof_sellingprice'
                # 'per_unit_sizedescription'
                # 'price_valid_from'
                'price_per_unit' : price_per_unit,
                # 'multi_buy_item_count'
                # 'multi_buy_items_price_total'
                'currency' : Currency,
                'breadcrumb' : breadcrumb,
                'pdp_url' : url,
                'variants' : variants,
                'product_description' : description,
                # 'instructions'
                # 'storage_instructions'
                # 'preparationinstructions'
                # 'instructionforuse'
                # 'country_of_origin'
                # 'allergens'
                # 'age_of_the_product'
                # 'age_recommendations'
                # 'flavour'
                # 'nutritions'
                # 'nutritional_information'
                # 'vitamins'
                # 'labelling'
                # 'grade'
                # 'region'
                # 'packaging'
                # 'receipies'
                # 'processed_food'
                # 'barcode'
                # 'frozen'
                # 'chilled'
                # 'organictype'
                # 'cooking_part'
                # 'handmade'
                # 'max_heating_temperature'
                # 'special_information'
                # 'label_information'
                # 'dimensions'
                # 'special_nutrition_purpose'
                'feeding_recommendation' : feeding_recommendation,
                # 'warranty'
                # 'color'
                # 'model_number'
                # 'material'
                # 'usp'
                # 'dosage_recommendation'
                # 'tasting_note'
                # 'food_preservation'
                # 'size'
                'rating' : rating,
                'review' : review,
                'file_name_1' : file_name_1,
                'image_url_1' : image_url_1,
                'file_name_2' : file_name_2,
                'image_url_2' : image_url_2,
                'file_name_3' : file_name_3,
                'image_url_3' : image_url_3,
                'file_name_4' : file_name_4,
                'image_url_4' : image_url_4,
                'file_name_5' : file_name_5,
                'image_url_5' : image_url_5,
                'file_name_6' : file_name_6,
                'image_url_6' : image_url_6,
                # 'competitor_product_key'
                # 'fit_guide'
                # 'occasion'
                # 'material_composition'
                # 'style'
                # 'care_instructions'
                # 'heel_type'
                # 'heel_height'
                # 'upc'
                # 'features'
                # 'dietary_lifestyle'
                # 'manufacturer_address'
                # 'importer_address'
                'distributor_address' :distributor_address,
                # 'vinification_details'
                # 'recycling_information'
                # 'return_address'
                # 'alchol_by_volume'
                # 'beer_deg'
                # 'netcontent'
                # 'netweight'
                'site_shown_uom' : product_name,
                'ingredients': ingredients,
                # 'random_weight_flag'
                'instock' : instock,
                # 'promo_limit'
                'product_unique_key' : product_unique_key,
                # 'multibuy_items_pricesingle'
                # 'perfect_match'
                # 'servings_per_pack'
                # 'warning'
                # 'suitable_for'
                # 'standard_drinks'
                # 'environmental'
                # 'grape_variety'
                # 'retail_limit'
                           
            }

            logging.info(product_info)
            try:
                self.parsed_collection.insert_one(product_info)
                logging.info("Agent information saved successfully!")
            except Exception as e:
                logging.error(f"Failed to save agent information: {str(e)}")

        else:
            logging.error(f"Failed to fetch URL: {url}. Status code: {response.status_code}")

    def parse_urls(self):
        while self.collection.count_documents({}) > 0:
            agent_doc = self.collection.find_one_and_delete({})
            agent_url = agent_doc["url"]
            self.parse_agent_page(agent_url)

parser = fressnapfParser()
parser.parse_urls()