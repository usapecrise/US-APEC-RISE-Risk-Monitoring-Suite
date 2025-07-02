import requests
import csv
import os

# Airtable credentials and table config
AIRTABLE_TOKEN = os.environ['AIRTABLE_TOKEN']
BASE_ID = 'app0Ljjhrp3lTTpTO'
TABLE_NAME = 'OC4 Market Growth'
VIEW_NAME = 'Grid view'

url = f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_NAME}?view={VIEW_NAME}"
headers = {"Authorization": f"Bearer {AIRTABLE_TOKEN}"}

print("Requesting:", url)

records = []
offset = None

while True:
    params = {}
    if offset:
        params['offset'] = offset
    response = requests.get(url, headers=headers, params=params).json()
    
    if 'records' not in response:
        print("❌ Airtable API Error:", response)
        exit(1)

    records.extend(response['records'])
    offset = response.get('offset')
    if not offset:
        break

# Write to OC4.csv
with open('OC4.csv', 'w', newline='', encoding='utf-8') as csvfile:
    if records:
        fieldnames = list(records[0]['fields'].keys())
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for record in records:
            writer.writerow(record['fields'])

print("✅ Export complete: OC4.csv")


