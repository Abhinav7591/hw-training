import requests
import re
import logging
from pymongo import MongoClient


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

base_url = "https://www.reecenichols.com"
referer_url = "https://www.reecenichols.com/roster/agents"
user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.99 Safari/537.36"

headers = {
    'Referer': referer_url,
    'User-Agent': user_agent
}

client = MongoClient("mongodb://localhost:27017/")
db = client["reecenichols"]
url_collection = db["agent_urls"]
url_collection.create_index([('url', 1)], unique=True)

page_number = 1

while True:
    url = f"https://www.reecenichols.com/CMS/CmsRoster/RosterSearchResults?layoutID=944&pageSize=10&pageNumber={page_number}&sortBy=firstname-asc"
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.text
        matches = re.findall(r'\/bio\/([^"]+)', data)
        if matches:
            for match in matches:
                clean_match = match.replace("\\", "")
                url = base_url + "/bio/" + clean_match
                
                try:
                    url_collection.insert_one({'url': url})
                    logging.info(f"Inserted URL: {url}")
                except Exception as e:
                    logging.error(f"Error inserting URL: {url}. {e}")
            page_number += 1
        else:
            logging.info(f"No matches found on page {page_number}. Pagination completed.")
            break
    else:
        logging.error(f"Failed to retrieve data for page {page_number}. Status code: {response.status_code}")
        break

logging.info("All pages processed.")
