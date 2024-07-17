from datetime import datetime


BASE_URL = 'https://www.sosandar.com/'
CATEGORY_URL = 'https://www.sosandar.com/clothing/'

MONGO_URI = "mongodb://localhost:27017/"
MONGODB_NAME = "sosander_2024_07_16"

MONGO_COLLECTION_URL = "sosander_product_url"
MONGO_COLLECTION_CATEGORY = "sosander_category_urls"
MONGO_COLLECTION_DATA = "sosander_product_data"
MONGO_COLLECTION_CRAWLER_ERROR = "sosander_crawler_error_urls"
MONGO_COLLECTION_PARSER_ERROR = "sosander_parser_error_urls"

TODAY_DATE = datetime.now().strftime('%Y-%m-%d')
FILE_NAME_CSV = f"sosandar_2024_07_18.csv"

HEADERS = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7,ml;q=0.6',
    'cache-control': 'max-age=0',
    'priority': 'u=0, i',
    'sec-ch-ua': '"Google Chrome";v="125", "Chromium";v="125", "Not.A/Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Linux"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'none',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
}
