import requests
import csv
import os
from urllib.parse import quote

# Airtable credentials and config
AIRTABLE_TOKEN = os.environ['AIRTABLE_TOKEN']
BASE_ID = 'app0Ljjhrp3lTTpTO'  # same base as OC4
TABLE_NAME = 'OC1 Policy Reforms'
VIEW_NAME = 'Grid view'  # or 'Export View' if you set one

encoded_table = quote(TABLE_NAME)
encoded_view = quote(VIEW_NAME)

url = f"https://api.airtable.com/v0/{BASE_ID}/{encoded_table}?view={encoded_view}"
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

with open('OC1.csv', 'w', newline='', encoding='utf-8') as csvfile:
    if records:
        fieldnames = list(records[0]['fields'].keys())
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for record in records:
            writer.writerow(record['fields'])

print("✅ Export complete: OC1.csv")
