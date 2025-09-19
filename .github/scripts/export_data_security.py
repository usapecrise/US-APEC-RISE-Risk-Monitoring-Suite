#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import csv
import os
from urllib.parse import quote
from datetime import datetime

# Airtable credentials and config
AIRTABLE_TOKEN = os.environ['AIRTABLE_TOKEN']
BASE_ID = 'app0Ljjhrp3lTTpTO'   # replace with your base ID
MAIN_TABLE = 'Data Security'    # name of your Airtable table
VIEW_NAME = 'Grid view'         # or whichever view you want to export

headers = {"Authorization": f"Bearer {AIRTABLE_TOKEN}"}

# Helper: fetch all records from a table
def fetch_all_records(table, view=None):
    url = f"https://api.airtable.com/v0/{BASE_ID}/{quote(table)}"
    if view:
        url += f"?view={quote(view)}"
    all_records = []
    offset = None

    while True:
        params = {}
        if offset:
            params['offset'] = offset
        response = requests.get(url, headers=headers, params=params).json()

        if 'records' not in response:
            print(f"❌ Error fetching {table}:", response)
            break

        all_records.extend(response['records'])
        offset = response.get('offset')
        if not offset:
            break

    print(f"✅ Fetched {len(all_records)} records from '{table}'")
    return all_records

# Step 1: Fetch main table records
main_records = fetch_all_records(MAIN_TABLE, view=VIEW_NAME)
print(f"🔍 Retrieved {len(main_records)} records from {MAIN_TABLE}")

# Step 2: Add timestamp for file change detection
timestamp = datetime.utcnow().isoformat()
for record in main_records:
    record['fields']['Last Updated'] = timestamp

# Step 3: Export to CSV
output_file = 'Data_Security.csv'
with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
    if main_records:
        all_fieldnames = set()
        for rec in main_records:
            all_fieldnames.update(rec['fields'].keys())
        fieldnames = list(all_fieldnames)

        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for rec in main_records:
            writer.writerow(rec['fields'])

print(f"✅ Export complete: {output_file}")
