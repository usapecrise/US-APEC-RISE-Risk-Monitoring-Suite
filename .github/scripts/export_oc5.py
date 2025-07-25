import requests
import csv
import os
from urllib.parse import quote
from datetime import datetime

# Airtable credentials and config
AIRTABLE_TOKEN = os.environ['AIRTABLE_TOKEN']
BASE_ID = 'app0Ljjhrp3lTTpTO'
MAIN_TABLE = 'OC5 Connections' 
VIEW_NAME = 'Grid view'

# Linked fields and their reference tables
LINKED_TABLES = {
    'Economy': 'Economy Reference List',
    'Workstream': 'Workstream Reference List',
}

# Fields to display from the linked reference tables
DISPLAY_FIELDS = {
    'Economy': 'Economy',
    'Workstream': 'Workstream',
}

headers = {"Authorization": f"Bearer {AIRTABLE_TOKEN}"}

# Fetch all records from an Airtable table
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

# Step 1: Build lookup maps for linked fields
linked_id_maps = {}
for field, table_name in LINKED_TABLES.items():
    records = fetch_all_records(table_name)
    display_field = DISPLAY_FIELDS[field]
    id_to_display = {
        rec['id']: rec['fields'].get(display_field, 'Unknown')
        for rec in records
    }
    linked_id_maps[field] = id_to_display

# Step 2: Fetch main OC5 records
main_records = fetch_all_records(MAIN_TABLE, view=VIEW_NAME)
print(f"🔍 Retrieved {len(main_records)} records from {MAIN_TABLE}")

# Step 3: Enrich records with readable names and timestamp
timestamp = datetime.utcnow().isoformat()
for record in main_records:
    fields = record['fields']
    for field_name in LINKED_TABLES.keys():
        linked_ids = fields.get(field_name, [])
        if isinstance(linked_ids, list):
            readable_names = [linked_id_maps[field_name].get(id, 'Unknown') for id in linked_ids]
            fields[f"{field_name} (Name)"] = ", ".join(readable_names)
    fields['Last Updated'] = timestamp  # Force change for GitHub push

# Step 4: Export to CSV
output_file = 'OC5.csv'
with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
    if main_records:
        fieldnames = [
            'Connection',
            'Indicator ID',
            'Economy (Name)',
            'Workstream (Name)',
            'Last Updated'
        ]

        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for rec in main_records:
            row = rec['fields']
            filtered_row = {key: row.get(key, '') for key in fieldnames}
            writer.writerow(filtered_row)

print(f"✅ Export complete: {output_file}")
