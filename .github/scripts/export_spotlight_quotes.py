import requests
import csv
import os
from urllib.parse import quote
from datetime import datetime, timezone

# Airtable credentials and config
AIRTABLE_TOKEN = os.environ['AIRTABLE_TOKEN']
BASE_ID = 'app0Ljjhrp3lTTpTO'
MAIN_TABLE = 'Spotlight Quotes'
VIEW_NAME = 'Grid view'
ECONOMY_TABLE = 'Economy Reference List'

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

# Step 1: Fetch Economy Reference List and build lookup
economy_records = fetch_all_records(ECONOMY_TABLE)
economy_lookup = {
    rec['id']: rec['fields'].get('Economy', '')
    for rec in economy_records
}

# Step 2: Fetch Spotlight Quotes
main_records = fetch_all_records(MAIN_TABLE, view=VIEW_NAME)

# Step 3: Build rows for CSV
rows = []
for i, rec in enumerate(main_records, start=1):
    f = rec.get("fields", {})

    # Economy is a linked list of record IDs
    economy_ids = f.get("Economy", [])
    if isinstance(economy_ids, list):
        economy_names = [economy_lookup.get(eid, eid) for eid in economy_ids]
        economy_str = ", ".join(economy_names)
    else:
        economy_str = economy_lookup.get(economy_ids, "")

    rows.append({
        "Quote ID": i,  # sequential ID
        "Quote Text": f.get("Quote Text", ""),
        "Organization": f.get("Organization", ""),
        "Economy": economy_str,
        "Workshop Title": f.get("Workshop Title", ""),
        "Last Exported": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    })

# Step 4: Export to CSV
output_file = "spotlight_quotes.csv"
with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)

print(f"✅ Export complete: {output_file}")
print("🔎 Preview of first 3 rows:")
for r in rows[:3]:
    print(r)
