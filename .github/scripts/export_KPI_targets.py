import requests
import csv
import os
from urllib.parse import quote
from datetime import datetime

# Airtable credentials and config
AIRTABLE_TOKEN = os.environ['AIRTABLE_TOKEN']
BASE_ID = 'app0Ljjhrp3lTTpTO'
MAIN_TABLE = 'KPI Targets'
VIEW_NAME = 'Grid view'

# ✅ Only export these specific columns (match Airtable field names exactly)
SELECTED_FIELDS = ['Indicator ID', 'Name', 'Type', 'Target', 'Unit']

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

# Step 2: Export to CSV
output_file = 'KPI_targets.csv'
with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=SELECTED_FIELDS)
    writer.writeheader()
    for rec in main_records:
        row = {field: rec['fields'].get(field, "") for field in SELECTED_FIELDS}
        writer.writerow(row)

print(f"✅ Export complete: {output_file}")
