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
        headers = ['Url','Size','Price','Quantity','Product_Name','Product_Summary','Brand','Product_Type','Country_Of_Orgin']
        
        with open(self.output_csv_path, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file, delimiter=',')
            writer.writerow(headers) 

            for doc in self.documents:
                Url = doc.get('Url', '')
                Size = doc.get('Size', '')
                Price = doc.get('Price', '')
                Quantity = doc.get('Quantity', '')
                Product_Name = doc.get('Product_Name', '')
                Product_Summary = doc.get('Product_Summary', '')
                Brand = doc.get('Brand', '')
                Product_Type = doc.get('Product_Type', '')
                Country_Of_Orgin = doc.get('Country_Of_Orgin', '')
                
                data = [
                    Url,
                    Size,
                    Price,
                    Quantity,
                    Product_Name,
                    Product_Summary,
                    Brand,
                    Product_Type,
                    Country_Of_Orgin                              
                ]
                
                writer.writerow(data)
                
        logging.info(f"Data saved to {self.output_csv_path}")

if __name__ == "__main__":
    
    output_csv = ExportData("carrefouruae_20240702.csv")
    output_csv.data_document()