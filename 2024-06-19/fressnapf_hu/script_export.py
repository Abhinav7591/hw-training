import csv
from pipeline import MongoPipeline
from settings import next_thursday

class ExportData:
    def __init__(self, output_csv_path):
        self.mongo_pipeline = MongoPipeline()
        self.parsed_collection = self.mongo_pipeline.db['product_details']
        self.documents = self.parsed_collection.find()
        self.output_csv_path = output_csv_path

    def data_document(self):
        headers = [
            'unique_id', 'competitor_name', 'store_name', 'store_addressline1', 'store_addressline2', 'store_suburb',
            'store_state', 'store_postcode', 'store_addressid', 'extraction_date', 'product_name', 'brand', 'brand_type',
            'grammage_quantity', 'grammage_unit', 'drained_weight', 'producthierarchy_level1', 'producthierarchy_level2',
            'producthierarchy_level3', 'producthierarchy_level4', 'producthierarchy_level5', 'producthierarchy_level6',
            'producthierarchy_level7', 'regular_price', 'selling_price', 'price_was', 'promotion_price', 'promotion_valid_from',
            'promotion_valid_upto', 'promotion_type', 'percentage_discount', 'promotion_description', 'package_sizeof_sellingprice',
            'per_unit_sizedescription', 'price_valid_from', 'price_per_unit', 'multi_buy_item_count', 'multi_buy_items_price_total',
            'currency', 'breadcrumb', 'pdp_url', 'variants', 'product_description', 'instructions', 'storage_instructions',
            'preparationinstructions', 'instructionforuse', 'country_of_origin', 'allergens', 'age_of_the_product',
            'age_recommendations', 'flavour', 'nutritions', 'nutritional_information', 'vitamins', 'labelling', 'grade', 'region',
            'packaging', 'receipies', 'processed_food', 'barcode', 'frozen', 'chilled', 'organictype', 'cooking_part', 'handmade',
            'max_heating_temperature', 'special_information', 'label_information', 'dimensions', 'special_nutrition_purpose',
            'feeding_recommendation', 'warranty', 'color', 'model_number', 'material', 'usp', 'dosage_recommendation',
            'tasting_note', 'food_preservation', 'size', 'rating', 'review', 'file_name_1', 'image_url_1', 'file_name_2', 'image_url_2',
            'file_name_3', 'image_url_3', 'file_name_4', 'image_url_4', 'file_name_5', 'image_url_5', 'file_name_6', 'image_url_6',
            'competitor_product_key', 'fit_guide', 'occasion', 'material_composition', 'style', 'care_instructions', 'heel_type',
            'heel_height', 'upc', 'features', 'dietary_lifestyle', 'manufacturer_address', 'importer_address', 'distributor_address',
            'vinification_details', 'recycling_information', 'return_address', 'alchol_by_volume', 'beer_deg', 'netcontent',
            'netweight', 'site_shown_uom', 'ingredients', 'random_weight_flag', 'instock', 'promo_limit', 'product_unique_key',
            'multibuy_items_pricesingle', 'perfect_match', 'servings_per_pack', 'warning', 'suitable_for', 'standard_drinks',
            'environmental', 'grape_variety', 'retail_limit'
        ]
        
        with open(self.output_csv_path, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file, delimiter='|')
            writer.writerow(headers) 

            for doc in self.documents:
                
                unique_id = doc.get('unique_id', '')
                competitor_name = doc.get('competitor_name', '')
                extraction_date = next_thursday
                product_name = doc.get('product_name', '')
                brand =  doc.get('brand', '')
                grammage_unit =  doc.get('grammage_unit', '')
                producthierarchy_level1 = doc.get('producthierarchy_level1', '')
                producthierarchy_level2 = doc.get('producthierarchy_level2', '')
                producthierarchy_level3 = doc.get('producthierarchy_level3', '')
                producthierarchy_level4 = doc.get('producthierarchy_level4', '')
                producthierarchy_level5 = doc.get('producthierarchy_level5', '')
                producthierarchy_level6 = doc.get('producthierarchy_level6', '')
                producthierarchy_level7 = doc.get('producthierarchy_level7', '')
                promotion_valid_from = doc.get('promotion_valid_from', '')
                promotion_valid_upto = doc.get('promotion_valid_upto', '')
                promotion_description = doc.get('promotion_description', '')
                price_valid_from = doc.get('price_valid_from', '')
                currency = doc.get('currency', '') 
                breadcrumb = doc.get('breadcrumb', '')
                pdp_url = doc.get('pdp_url', '')
                age_recommendations = doc.get('age_recommendations', '')
                packaging = doc.get('packaging', '')
                special_information = doc.get('special_information', '')
                color = doc.get('color', '')
                material = doc.get('material', '')
                tasting_note = doc.get('tasting_note', '')
                size = doc.get('size', '') 
                rating = doc.get('rating', '') 
                review = doc.get('review', '') 
                file_name_1 = doc.get('file_name_1', '') 
                image_url_1 = doc.get('image_url_1', '') 
                file_name_2 = doc.get('file_name_2', '') 
                image_url_2 = doc.get('image_url_2', '') 
                file_name_3 = doc.get('file_name_3', '') 
                image_url_3 = doc.get('image_url_3', '')
                file_name_4 = doc.get('file_name_4', '') 
                image_url_4 = doc.get('image_url_4', '') 
                file_name_5 = doc.get('file_name_5', '') 
                image_url_5 = doc.get('image_url_5', '') 
                file_name_6 = doc.get('file_name_6', '') 
                image_url_6 = doc.get('image_url_6', '') 
                site_shown_uom = doc.get('site_shown_uom', '')
                instock = doc.get('instock', '')
                product_unique_key = doc.get('product_unique_key', '')
                store_name = ''
                store_addressline1 = ''
                store_addressline2 = ''
                store_suburb = ''
                store_state = ''
                store_postcode = ''
                store_addressid = ''
                brand_type = ''
                drained_weight = ''
                promotion_type = '' 
                percentage_discount = ''
                package_sizeof_sellingprice = ''
                per_unit_sizedescription = ''
                multi_buy_item_count = ''
                multi_buy_items_price_total = '' 
                variants = ''
                instructions = ''
                storage_instructions = '' 
                preparationinstructions = ''
                instructionforuse = ''
                country_of_origin = ''
                allergens = ''
                age_of_the_product = '' 
                flavour = ''
                nutritions = ''
                nutritional_information = '' 
                vitamins = ''
                labelling = ''
                grade = ''
                region = ''
                receipies = '' 
                processed_food = '' 
                barcode = ''
                frozen = ''
                chilled = ''
                organictype = '' 
                cooking_part = ''
                handmade = ''
                max_heating_temperature = ''
                label_information = ''
                dimensions = ''
                special_nutrition_purpose = '' 
                feeding_recommendation = ''
                warranty = ''
                model_number = ''
                usp = ''
                dosage_recommendation = '' 
                food_preservation = ''
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
                distributor_address = '' 
                vinification_details = '' 
                recycling_information = '' 
                return_address = '' 
                alchol_by_volume = '' 
                beer_deg = '' 
                netcontent = '' 
                netweight = '' 
                ingredients = '' 
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
                retail_limit = ''   
                
                product_description = doc.get('product_description', '')
                if isinstance(product_description, str):
                    product_description = (
                        product_description
                        .replace('|', '/|')  
                        .replace('\n', '')  
                        .replace('<br>', '')  
                        .replace('\t', '')     
                    )
            
                grammage_quantity = doc.get('grammage_quantity', '').replace(',', '.')                       
                price_per_unit = doc.get('price_per_unit', '').replace(',', '.') 
   
                
                if '_id' in doc:
                    del doc['_id']
                                          
                price_keys = ['regular_price', 'selling_price', 'price_was', 'promotion_price']
                for key in price_keys:
                    if key in doc and doc[key] is doc[key] != '':
                        if doc[key].replace('.', '', 1).isdigit():
                            doc[key] = float(doc[key])
                                            
                if not doc.get('price_per_unit', ''):
                    continue

                reg_price = doc.get('regular_price', '')
                sell_price = doc.get('selling_price', '')

                if reg_price and reg_price != sell_price:
                    doc['regular_price'] = reg_price
                    doc['selling_price'] = sell_price
                    doc['price_was'] = reg_price
                    doc['promotion_price'] = sell_price
                else:
                    doc['regular_price'] = sell_price
                    doc['selling_price'] = sell_price
                    doc['price_was'] = ''
                    doc['promotion_price'] = ''
                    doc['promo_desc'] = ''
                
                regular_price = doc['regular_price']
                selling_price = doc['selling_price']
                price_was = doc['price_was']
                promotion_price = doc['promotion_price'] 
                
                
                # for key in doc:
                #     if isinstance(doc[key], str):
                #         doc[key] = doc[key].replace('|', '/|').replace('\n', ' ').replace('<br>', ' ').replace('\t','')

                row =[
                    unique_id,
                    competitor_name,
                    store_name,
                    store_addressline1,
                    store_addressline2,
                    store_suburb,
                    store_state,
                    store_postcode,
                    store_addressid,
                    extraction_date,
                    product_name,
                    brand,
                    brand_type,
                    grammage_quantity,
                    grammage_unit,
                    drained_weight,
                    producthierarchy_level1,
                    producthierarchy_level2,
                    producthierarchy_level3,
                    producthierarchy_level4,
                    producthierarchy_level5,
                    producthierarchy_level6,
                    producthierarchy_level7,
                    regular_price, 
                    selling_price, 
                    price_was, 
                    promotion_price, 
                    promotion_valid_from, 
                    promotion_valid_upto, 
                    promotion_type, 
                    percentage_discount, 
                    promotion_description, 
                    package_sizeof_sellingprice, 
                    per_unit_sizedescription, 
                    price_valid_from, 
                    price_per_unit, 
                    multi_buy_item_count, 
                    multi_buy_items_price_total, 
                    currency, 
                    breadcrumb, 
                    pdp_url, 
                    variants, 
                    product_description, 
                    instructions, 
                    storage_instructions, 
                    preparationinstructions, 
                    instructionforuse, 
                    country_of_origin, 
                    allergens, 
                    age_of_the_product, 
                    age_recommendations, 
                    flavour, 
                    nutritions, 
                    nutritional_information, 
                    vitamins, 
                    labelling, 
                    grade, 
                    region, 
                    packaging, 
                    receipies, 
                    processed_food, 
                    barcode, 
                    frozen, 
                    chilled, 
                    organictype, 
                    cooking_part, 
                    handmade, 
                    max_heating_temperature, 
                    special_information, 
                    label_information, 
                    dimensions, 
                    special_nutrition_purpose, 
                    feeding_recommendation, 
                    warranty, 
                    color, 
                    model_number, 
                    material, 
                    usp, 
                    dosage_recommendation, 
                    tasting_note, 
                    food_preservation, 
                    size, 
                    rating, 
                    review, 
                    file_name_1, 
                    image_url_1, 
                    file_name_2, 
                    image_url_2, 
                    file_name_3, 
                    image_url_3,
                    file_name_4, 
                    image_url_4, 
                    file_name_5, 
                    image_url_5, 
                    file_name_6, 
                    image_url_6, 
                    competitor_product_key, 
                    fit_guide, 
                    occasion, 
                    material_composition, 
                    style, 
                    care_instructions, 
                    heel_type, 
                    heel_height, 
                    upc, 
                    features, 
                    dietary_lifestyle, 
                    manufacturer_address, 
                    importer_address, 
                    distributor_address, 
                    vinification_details, 
                    recycling_information, 
                    return_address, 
                    alchol_by_volume, 
                    beer_deg, 
                    netcontent, 
                    netweight, 
                    site_shown_uom, 
                    ingredients, 
                    random_weight_flag, 
                    instock, 
                    promo_limit, 
                    product_unique_key, 
                    multibuy_items_pricesingle, 
                    perfect_match, 
                    servings_per_pack,
                    warning, 
                    suitable_for, 
                    standard_drinks, 
                    environmental,
                    grape_variety, 
                    retail_limit,    
                ]
                writer.writerow(row)
                
        print(f"Data saved to {self.output_csv_path}")

if __name__ == "__main__":
    output_csv = ExportData("DataHut_HU_Fressnapf_FullDump_20240620.csv")
    output_csv.data_document()


