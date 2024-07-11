from pipeline import MongoPipeline
import csv
import logging
from settings import next_thursday


class ExportData:
    def __init__(self, output_csv_path):
        self.mongo_pipeline = MongoPipeline()
        self.parsed_collection = self.mongo_pipeline.db['dubizzle_property_data']
        self.documents = self.parsed_collection.find()
        self.output_csv_path = output_csv_path
        
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    def data_document(self):
        headers = ['Url','Categories','Product Name','Brand','Price','Location','Seller Type','Poster Year','Furnishing','Purpose','Amenities',
                   'Type','Property Age','Validated Information','Floor Size','No.of Beds','No.of Bathroom','Property History','Completion',
                   'Tier/Segment','Usage','Geolocation','Property Type','Unit Type','District Name','AddressLocality','AddressRegion','Property/Item ID',
                   'ExtractionDate','AvailabilityDate','Property Description','Currency']
        
        with open(self.output_csv_path, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file, delimiter=',')
            writer.writerow(headers) 

            for doc in self.documents:
                url = doc.get('url', '')
                property_name = doc.get('property_name', '')
                floor_size = doc.get('floor_size', '')
                unit_type = doc.get('unit_type', '')
                purpose = doc.get('purpose', '') 
                types = doc.get('type', '')
                location = doc.get('location', '')
                baths = doc.get('baths', '')
                beds = doc.get('beds', '')
                completion = doc.get('completion', '')
                poster_year = doc.get('poster_year', '')
                price = doc.get('price', '')
                currency = doc.get('currency', '')
                property_age = doc.get('property_age', '')
                categories = doc.get('categories', '')
                furnishing = doc.get('furnishing', '')
                item_id = doc.get('item_id', '')
                validated_information = doc.get('validated_information', '')
                property_type = doc.get('property_type', '')
                description = doc.get('description', '')
                description = (description
                                .replace('<br>', '')
                                .replace('<li>', '')
                                .replace('</li>', '')
                                .replace('<strong>', '')
                                .replace('</strong>', '')
                                .replace('<ul>','')
                                .replace('</ul>','')
                                .replace('\n', ' ')
                                .strip())
                amenities = doc.get('amenities', '')
                goelocation = doc.get('goelocation', '')
                
                history_info = doc.get('history_info', "")
                if isinstance(history_info, list) and not history_info:
                    history_info = ""
                    
                extraction_date = next_thursday
                brand = ''
                seller_type = ''
                segment = ''
                usage = ''
                district_name =''
                address_locality = ''
                address_region = ''              
                availability_date = ''
                
                data = [
                    url,
                    categories,
                    property_name,
                    brand,
                    price,
                    location,
                    seller_type,
                    poster_year,
                    furnishing,
                    purpose,
                    amenities,
                    types,
                    property_age,
                    validated_information,
                    floor_size,
                    beds,
                    baths,
                    history_info,
                    completion,
                    segment,
                    usage,
                    goelocation,
                    property_type,
                    unit_type,
                    district_name,
                    address_locality,
                    address_region,
                    item_id,
                    extraction_date,
                    availability_date,
                    description,
                    currency 
                ]
                
                writer.writerow(data)
                
        logging.info(f"Data saved to {self.output_csv_path}")

if __name__ == "__main__":
    
    output_csv = ExportData("dubizzle_2024_07_11.csv")
    output_csv.data_document()