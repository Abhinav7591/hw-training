import csv
import datetime
import re
from pipeline import product_details_collection

class ExportData:
    def __init__(self, collection_name, output_csv_path):
        self.collection = product_details_collection("fressnapf", collection_name)
        self.documents = self.collection.find()
        self.output_csv_path = output_csv_path

    def next_thursday(self):
        today = datetime.date.today()
        days_until_thursday = (3 - today.weekday() + 7) % 7
        next_thursday_date = today + datetime.timedelta(days=days_until_thursday)
        return next_thursday_date.strftime("%Y-%m-%d")

    def data_document(self):
        with open(self.output_csv_path, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file, delimiter='|')
            header_written = False

            for doc in self.documents:
                doc_dict = dict(doc)

                for key, value in doc_dict.items():
                    if isinstance(value, str):
                        doc_dict[key] = value.replace('|', '/|')

                doc_dict['extraction_date'] = self.next_thursday()

                if '_id' in doc_dict:
                    del doc_dict['_id']

                if 'review' in doc_dict:
                    doc_dict['review'] = str(doc_dict['review'])

                price_keys = ['regular_price', 'selling_price', 'price_was', 'promotion_price']
                for key in price_keys:
                    if key in doc_dict and doc_dict[key] != '':
                        doc_dict[key] = float(doc_dict[key])

                if 'variants' in doc_dict and isinstance(doc_dict['variants'], dict):
                    if 'variant' in doc_dict['variants'] and isinstance(doc_dict['variants']['variant'], list) and not doc_dict['variants']['variant']:
                        doc_dict['variants'] = ''

                if 'feeding_recommendation' in doc_dict:
                    feeding_recommendation = doc_dict['feeding_recommendation']
                    if 'feeding_suggestion' not in feeding_recommendation or not feeding_recommendation['feeding_suggestion']:
                        if 'table_data' not in feeding_recommendation or not feeding_recommendation['table_data']:
                            doc_dict['feeding_recommendation'] = ''

                self.adjust_prices(doc_dict)
                self.stuck_grammage_info(doc_dict)

                if not header_written:
                    header = doc_dict.keys()
                    writer.writerow(header)
                    header_written = True

                writer.writerow(doc_dict.values())

        print(f"Data saved to {self.output_csv_path}")

    def adjust_prices(self, doc_dict):
        promo_desc = doc_dict.get('promotion_description', '')
        reg_price = doc_dict.get('regular_price', '')
        sell_price = doc_dict.get('selling_price', '')
        price_was = doc_dict.get('price_was', '')
        promo_price = doc_dict.get('promotion_price', '')

        if not promo_desc:
            if reg_price == sell_price:
                doc_dict['regular_price'] = sell_price
                doc_dict['selling_price'] = sell_price
                doc_dict['price_was'] = ''
                doc_dict['promotion_price'] = ''
            else:
                doc_dict['regular_price'] = reg_price
                doc_dict['selling_price'] = sell_price
                doc_dict['price_was'] = reg_price
                doc_dict['promotion_price'] = sell_price
        else:
            if reg_price == sell_price:
                doc_dict['regular_price'] = sell_price
                doc_dict['selling_price'] = sell_price
                doc_dict['price_was'] = ''
                doc_dict['promotion_price'] = ''
            else:
                doc_dict['regular_price'] = reg_price
                doc_dict['selling_price'] = reg_price
                doc_dict['price_was'] = reg_price
                doc_dict['promotion_price'] = sell_price


    def stuck_grammage_info(self, doc_dict):
        pattern = r"(\d+)\s*"
        product_name = doc_dict.get('product_name', '')

        if "stück" in product_name.lower():
            match = re.search(pattern, product_name)
            if match:
                grammage_quantity = match.group(1)
                grammage_unit = 'stück'
                doc_dict['grammage_quantity'] = grammage_quantity
                doc_dict['grammage_unit'] = grammage_unit

        if 'grammage_quantity' in doc_dict and (doc_dict['grammage_quantity'] == " "):
            doc_dict['grammage_quantity'] = 1

        if 'grammage_unit' in doc_dict and (doc_dict['grammage_unit'] == " "):
            doc_dict['grammage_unit'] = 'stück'


if __name__ == "__main__":
    processor = ExportData("product_details", "fressnapf_data.csv")
    processor.data_document()
