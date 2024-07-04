from pipeline import MongoPipeline
import csv
import logging


class ExportData:
    def __init__(self, output_csv_path):
        self.mongo_pipeline = MongoPipeline()
        self.parsed_collection = self.mongo_pipeline.db['carrefouruae_product_data']
        self.documents = self.parsed_collection.find()
        self.output_csv_path = output_csv_path
        
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    def data_document(self):
        headers = ['Url','Quantity','Product Name','Pack Size','Price','Origin','Item Description','Sellers','Product Type','Brand','Offers & Promotions']
        
        with open(self.output_csv_path, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file, delimiter=',')
            writer.writerow(headers) 

            for doc in self.documents:
                url = doc.get('Url', '')
                quantity = doc.get('Quantity', '')
                product_name = doc.get('Product_Name', '')
                pakage_size = doc.get('package_size', '')
                price = doc.get('Price', '')
                price = float(price) 
                orgin = doc.get('Orgin', '')
                item_description = doc.get('Item_Description', '').replace('\n','')
                sellers = doc.get('Sellers', '')
                product_type = doc.get('Product_Type', '')
                brand = doc.get('Brand', '')
                Offers_Promotions = doc.get('Offers_Promotions', '')
                
                data = [
                    url,
                    quantity,
                    product_name,
                    pakage_size,
                    price,
                    orgin,
                    item_description,
                    sellers,
                    product_type,
                    brand,
                    Offers_Promotions      
                ]
                
                writer.writerow(data)
                
        logging.info(f"Data saved to {self.output_csv_path}")

if __name__ == "__main__":
    
    output_csv = ExportData("carrefouruae_20240702.csv")
    output_csv.data_document()