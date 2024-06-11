import csv
import datetime
from pipeline import product_details_collection

class ExportData:
    def __init__(self, collection_name, output_csv_path):
        self.collection = product_details_collection("mueller", collection_name)
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
                        doc_dict[key] = value.replace('|', '/|').replace('\n', ' ')
                        if key == 'price_per_unit':
                            doc_dict['price_per_unit'] = value.replace(',', '.')

                doc_dict['extraction_date'] = self.next_thursday()

                if '_id' in doc_dict:
                    del doc_dict['_id']

                price_keys = ['regular_price', 'selling_price', 'price_was', 'promotion_price']

                for key in price_keys:
                    if key in doc_dict and doc_dict[key] is not None and doc_dict[key] != '':
                        if doc_dict[key].replace('.', '', 1).isdigit():
                            doc_dict[key] = float(doc_dict[key])

                if 'variants' not in doc_dict or not doc_dict['variants']:
                    doc_dict['variants'] = ''

                if 'nutritional_information' not in doc_dict or not doc_dict['nutritional_information']:
                    doc_dict['nutritional_information'] = ''


                promo_desc = doc_dict.get('promotion_description', '')
                reg_price = doc_dict.get('regular_price', '')
                sell_price = doc_dict.get('selling_price', '')

                if promo_desc == "":
                    if reg_price == sell_price:
                        doc_dict['regular_price'] = sell_price
                        doc_dict['selling_price'] = reg_price
                        doc_dict['price_was'] = ''
                        doc_dict['promotion_price'] = ''
                    else:
                        doc_dict['regular_price'] = sell_price
                        doc_dict['selling_price'] = sell_price
                        doc_dict['price_was'] = reg_price
                        doc_dict['promotion_price'] = reg_price
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

                if not header_written:
                    header = doc_dict.keys()
                    writer.writerow(header)
                    header_written = True

                writer.writerow(doc_dict.values())

        print(f"Data saved to {self.output_csv_path}")

if __name__ == "__main__":
    processor = ExportData("product_details", "mueller_data2.csv")
    processor.data_document()
