import requests
import json
import logging
from pymongo import MongoClient, errors

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

client = MongoClient('mongodb://localhost:27017/') 
db = client['longandfoster']
collection = db['agent_urls']
collection.create_index("url", unique=True)


base_url = "https://www.longandfoster.com/include/ajax/api.aspx?op=SearchAgents&firstname=&lastname=&page={page}&pagesize=200"
headers = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Referer": "https://www.longandfoster.com/pages/real-estate-agent-office-search",
    "Accept": "application/json, text/javascript, */*; q=0.01",
}

page = 1
while True:
    url = base_url.format(page=page)
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        data = response.json()
        entities = json.loads(data.get("Entity", "[]"))
 
        if not entities:
            logging.info("No more entities found. Stopping.")
            break

        for entity in entities:
            display_name = entity.get("DisplayName")
            person_id = entity.get("PersonID")
            
            if display_name and person_id:
                agent_url = f"https://www.longandfoster.com/AgentSearch/{display_name.replace(' ', '')}-{person_id}"
                logging.info(f"URL: {agent_url}")
                try:
                    collection.insert_one({"url": agent_url})
                except errors.DuplicateKeyError:
                    logging.warning(f"Duplicate URL found and skipped: {agent_url}")
        page += 1
    else:
        logging.error(f"Failed to fetch data from the URL: {url}")
        break

