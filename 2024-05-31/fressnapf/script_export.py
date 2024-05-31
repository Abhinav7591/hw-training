import csv
from pipeline import product_details_collection
import datetime


def next_thursday():
    today = datetime.date.today()
    days_until_thursday = (3 - today.weekday() + 7) % 7
    next_thursday_date = today + datetime.timedelta(days=days_until_thursday)
    return next_thursday_date.strftime("%Y-%m-%d")

collection = product_details_collection("fressnapff", "product_details")
documents = collection.find()

output_csv_path = "fressnapf_datas.csv"

with open(output_csv_path, mode='w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file, delimiter='|')
    header_written = False
    
    for doc in documents:
        doc_dict = dict(doc)        

        doc_dict['extraction_date'] = next_thursday()

        if '_id' in doc_dict:
            del doc_dict['_id']

        if 'review' in doc_dict:
            doc_dict['review'] = str(doc_dict['review'])

        if 'selling_price' in doc_dict:
            doc_dict['selling_price'] = float(doc_dict['selling_price'])

        if 'price_was' in doc_dict and doc_dict['price_was'] != '':
            doc_dict['price_was'] = float(doc_dict['price_was'])

        if 'promotion_price' in doc_dict and doc_dict['promotion_price'] != '':
            doc_dict['promotion_price'] = float(doc_dict['promotion_price'])      

        if 'variants' in doc_dict and isinstance(doc_dict['variants'], dict):
            if 'variant' in doc_dict['variants'] and isinstance(doc_dict['variants']['variant'], list) and not doc_dict['variants']['variant']:
                doc_dict['variants'] = ''

        if 'feeding_recommendation' in doc_dict:
            feeding_recommendation = doc_dict['feeding_recommendation']
            if 'feeding_suggestion' not in feeding_recommendation or not feeding_recommendation['feeding_suggestion']:
                if 'table_data' not in feeding_recommendation or not feeding_recommendation['table_data']:
                    doc_dict['feeding_recommendation'] = ''

        # promo_desc = doc_dict.get('promotion_description', '')
        # reg_price = doc_dict.get('regular_price', '')
        # sell_price = doc_dict.get('selling_price', '')
        # price_was = doc_dict.get('price_was', '')
        # promo_price = doc_dict.get('promotion_price', '')

        # if not promo_desc:
        #     if reg_price == '' and sell_price == '':
        #         doc_dict['regular_price'] = ''
        #         doc_dict['selling_price'] = ''
        #         doc_dict['price_was'] = ''
        #         doc_dict['promotion_price'] = ''
        #     elif reg_price == sell_price:
        #         doc_dict['regular_price'] = ''
        #         doc_dict['selling_price'] = sell_price
        #         doc_dict['price_was'] = ''
        #         doc_dict['promotion_price'] = ''
        #     else:
        #         doc_dict['regular_price'] = reg_price
        #         doc_dict['selling_price'] = sell_price
        #         doc_dict['price_was'] = reg_price
        #         doc_dict['promotion_price'] = ''
        # else:
        #     if reg_price == '' and sell_price == '':
        #         doc_dict['regular_price'] = ''
        #         doc_dict['selling_price'] = ''
        #         doc_dict['price_was'] = ''
        #         doc_dict['promotion_price'] = ''
        #     elif reg_price == sell_price:
        #         doc_dict['regular_price'] = ''
        #         doc_dict['selling_price'] = sell_price
        #         doc_dict['price_was'] = ''
        #         doc_dict['promotion_price'] = ''
        #     else:
        #         doc_dict['regular_price'] = reg_price
        #         doc_dict['selling_price'] = sell_price
        #         doc_dict['price_was'] = reg_price
        #         doc_dict['promotion_price'] = sell_price



        if not header_written:
            header = doc_dict.keys()
            writer.writerow(header)
            header_written = True

        writer.writerow(doc_dict.values())

print(f"Data saved to {output_csv_path}")
