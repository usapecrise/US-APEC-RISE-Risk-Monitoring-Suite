import requests
import csv
import os
from urllib.parse import quote
from datetime import datetime

# Airtable credentials and config
AIRTABLE_TOKEN = os.environ['AIRTABLE_TOKEN']
BASE_ID = 'app0Ljjhrp3lTTpTO'
MAIN_TABLE = 'OT5 Private Sector Resources'
VIEW_NAME = 'Grid view'

# Linked tables and display fields
LINKED_TABLES = {
    'Economy': 'Economy Reference List',
    'Workstream': 'Workstream Reference List',
}

DISPLAY_FIELDS = {
    'Economy': 'Economy',
    'Workstream': 'Workstream',
}

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

    return all_records

# Step 1: Build lookup maps for linked fields
linked_id_maps = {}
for field, table in LINKED_TABLES.items():
    records = fetch_all_records(table)
    display_field = DISPLAY_FIELDS[field]
    id_to_display = {
        rec['id']: rec['fields'].get(display_field, 'Unknown')
        for rec in records
    }
    linked_id_maps[field] = id_to_display

# Step 2: Fetch main OT5 records
main_records = fetch_all_records(MAIN_TABLE, view=VIEW_NAME)

# Step 3: Resolve linked record IDs to readable names
for record in main_records:
    fields = record['fields']
    for field_name in LINKED_TABLES.keys():
        linked_ids = fields.get(field_name, [])
        if isinstance(linked_ids, list):
            readable_names = [linked_id_maps[field_name].get(id, 'Unknown') for id in linked_ids]
            fields[f"{field_name} (Name)"] = ", ".join(readable_names)
        elif isinstance(linked_ids, str):
            fields[f"{field_name} (Name)"] = linked_id_maps[field_name].get(linked_ids, 'Unknown')

# Step 4: Export to CSV with only selected fields
output_file = 'OT5.csv'
fieldnames = [
    'Firm (Name)',
    'Economy (Name)',
    'Workstream (Name)',
    'Amount',
    'Fiscal Year',
    'U.S. FAOs Addressed',
    'Resource Type',
    'Resource Origin',
    'Export Timestamp'  # New column to ensure uniqueness
]

timestamp = datetime.utcnow().isoformat()

with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for rec in main_records:
        row = {field: rec['fields'].get(field, '') for field in fieldnames}
        row['Export Timestamp'] = timestamp  # Add the timestamp
        writer.writerow(row)

print(f"✅ Export complete: {output_file}")
