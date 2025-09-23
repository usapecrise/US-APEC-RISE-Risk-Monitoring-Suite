import requests
import csv
import os
from urllib.parse import quote
from datetime import datetime, timezone

# Airtable credentials and config
AIRTABLE_TOKEN = os.environ['AIRTABLE_TOKEN']
BASE_ID = 'app0Ljjhrp3lTTpTO'
MAIN_TABLE = 'Spotlight Quotes'   # table name (not table ID is fine here)
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

# Step 1: Fetch Spotlight Quotes records
main_records = fetch_all_records(MAIN_TABLE, view=VIEW_NAME)
print(f"🔍 Retrieved {len(main_records)} records from {MAIN_TABLE}")

# Step 2: Build rows for CSV
rows = []
for i, rec in enumerate(main_records, start=1):
    f = rec.get("fields", {})
    rows.append({
        "Quote ID": i,  # generate sequential ID (1,2,3…)
        "Quote Text": f.get("Quote Text", ""),
        "Organization": f.get("Organization", ""),
        "Economy": f.get("Economy", ""),
        "Workshop Title": f.get("Workshop Title", ""),
        "Last Exported": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    })

# Step 3: Export to CSV
output_file = "spotlight_quotes.csv"
with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)

print(f"✅ Export complete: {output_file}")
print("🔎 Preview of first 3 rows:")
for r in rows[:3]:
    print(r)
