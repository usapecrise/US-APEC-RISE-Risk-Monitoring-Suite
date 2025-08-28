import requests
import csv
import os
from urllib.parse import quote
from datetime import datetime, timezone

# Airtable credentials and config
AIRTABLE_TOKEN = os.environ['AIRTABLE_TOKEN']
BASE_ID = 'app0Ljjhrp3lTTpTO'
MAIN_TABLE = 'KPI Targets'
VIEW_NAME = 'Grid view'

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

# Step 2: Collect all possible field names
all_fields = set()
for rec in main_records:
    all_fields.update(rec.get('fields', {}).keys())
all_fields = list(all_fields)

# Add timestamp field
all_fields.append("Last Exported")

print(f"📋 Detected {len(all_fields)} fields (including Last Exported): {all_fields}")

# Step 3: Export to CSV
output_file = 'KPI_Targets.csv'
timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=all_fields)
    writer.writeheader()
    for i, rec in enumerate(main_records):
        row = rec.get('fields', {}).copy()
        row["Last Exported"] = timestamp
        writer.writerow(row)

        # 🔎 Debug preview: print first 3 rows
        if i < 3:
            print("DEBUG row:", row)

print(f"✅ Export complete: {output_file}")

