import requests
import csv
import os
from urllib.parse import quote
from datetime import datetime

# Airtable credentials and config
AIRTABLE_TOKEN = os.environ['AIRTABLE_TOKEN']
BASE_ID        = 'app0Ljjhrp3lTTpTO'
MAIN_TABLE     = 'Feedback Form Entries'
VIEW_NAME      = 'Grid view'

headers = {"Authorization": f"Bearer {AIRTABLE_TOKEN}"}

def fetch_all_records(table, view=None):
    url = f"https://api.airtable.com/v0/{BASE_ID}/{quote(table)}"
    params, all_records, offset = ({}, [], None)

    if view:
        params['view'] = view

    while True:
        if offset:
            params['offset'] = offset
        resp = requests.get(url, headers=headers, params=params).json()

        if 'records' not in resp:
            print(f"❌ Error fetching {table}:", resp)
            break

        all_records.extend(resp['records'])
        offset = resp.get('offset')
        if not offset:
            break

    print(f"✅ Fetched {len(all_records)} records from '{table}'")
    return all_records

# Step 1: Fetch your main Feedback Form records
main_records = fetch_all_records(MAIN_TABLE, view=VIEW_NAME)
print(f"🔍 Retrieved {len(main_records)} records from {MAIN_TABLE}")

# Step 2: Stamp each record with a timestamp so CSV always changes
timestamp = datetime.utcnow().isoformat()
for record in main_records:
    record['fields']['Last Updated'] = timestamp

# Step 3: Export everything to CSV
output_file = 'Feedback_Form_Data.csv'
with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
    # build the full set of field names dynamically
    fieldnames = set()
    for rec in main_records:
        fieldnames.update(rec['fields'].keys())
    fieldnames = list(fieldnames)

    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for rec in main_records:
        writer.writerow(rec['fields'])

print(f"✅ Export complete: {output_file}")

