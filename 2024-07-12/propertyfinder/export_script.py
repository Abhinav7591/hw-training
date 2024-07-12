from pipeline import MongoPipeline
import csv
import logging
from settings import next_thursday


class ExportData:
    def __init__(self, output_csv_path):
        self.mongo_pipeline = MongoPipeline()
        self.parsed_collection = self.mongo_pipeline.db['Propertyfinder_property_data']
        self.documents = self.parsed_collection.find()
        self.output_csv_path = output_csv_path
        
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    def data_document(self):
        headers = ['Url','Category','Product Name','Property Agent Name','Location','Price','Geolocation','Property Type','Unit Type','District Name',
                   'AddressLocality','AddressRegion','Type','Property/Item ID','ExtractionDate','AvailabilityDate','Property Name','Property Description',
                   'Amenities','No.of Bedrooms','No.of Bathroom','Residential Insights','Property Size','Currency']
        
        with open(self.output_csv_path, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file, delimiter=',')
            writer.writerow(headers) 

            for doc in self.documents:
                url = doc.get('url', '')
                category = doc.get('category', '')
                product_name = doc.get('product_name', '')
                agent_name = doc.get('agent_name', '')
                location = doc.get('location', '')
                price = doc.get('price', '')
                if not price:
                    continue
                geolocation = doc.get('geolocation', '')
                property_type = doc.get('property_type', '')
                unit_type = doc.get('unit_type', '')
                address_locality = doc.get('address_locality', '')
                address_region = doc.get('address_region', '')
                type = doc.get('type', '')
                itemid = doc.get('itemid', '')
                extarction_date = next_thursday
                availability_date = doc.get('availability_date', '')
                property_name = doc.get('property_name', '')
                property_description = doc.get('property_description', '')
                amenities = doc.get('amenities', '')
                bedrooms = doc.get('bedrooms', '')
                bathrooms = doc.get('bathrooms', '')
                property_size = doc.get('property_size', '')
                currency = doc.get('currency', '')
                destrict_name= ''
                insights = ''

                
                data = [
                    url,
                    category,
                    product_name,
                    agent_name,
                    location,
                    price,
                    geolocation,
                    property_type,
                    unit_type,
                    destrict_name,
                    address_locality,
                    address_region,
                    type,
                    itemid,
                    extarction_date,
                    availability_date,
                    property_name,
                    property_description,
                    amenities,
                    bedrooms,
                    bathrooms,
                    insights,
                    property_size,
                    currency
                ]
                
                writer.writerow(data)
                
        logging.info(f"Data saved to {self.output_csv_path}")

if __name__ == "__main__":
    
    output_csv = ExportData("propertyfinder_2024_07_12.csv")
    output_csv.data_document()