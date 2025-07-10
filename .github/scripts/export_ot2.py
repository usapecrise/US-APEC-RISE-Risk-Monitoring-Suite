import requests
import csv
import os
from urllib.parse import quote

# Airtable credentials and config
AIRTABLE_TOKEN = os.environ['AIRTABLE_TOKEN']
BASE_ID = 'app0Ljjhrp3lTTpTO'
MAIN_TABLE = 'OT2 Private Sector Engagements'
VIEW_NAME = 'Grid view'

# Linked table names
LINKED_TABLES = {
    'Workstream': 'Workstream Reference List',
    'Economy': 'Economy Reference List',
    'Enagement': 'Workshop Reference List',
    'Firm': 'OT4 Private Sector Firms'
}

# Fields to display from the linked tables
DISPLAY_FIELDS = {
    'Workstream': 'Workstream',
    'Economy': 'Economy',
    'Engagement': 'Workshop',
    'Firm': 'Firm'
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

# Step 1: Fetch linked records and build lookup maps
linked_id_maps = {}
for field, table_name in LINKED_TABLES.items():
    records = fetch_all_records(table_name)
    display_field = DISPLAY_FIELDS[field]
    id_to_display = {
        rec['id']: rec['fields'].get(display_field, 'Unknown')
        for rec in records
    }
    linked_id_maps[field] = id_to_display

# Step 2: Fetch main OT2 records
main_records = fetch_all_records(MAIN_TABLE, view=VIEW_NAME)

# Step 3: Resolve linked field IDs to display names
for record in main_records:
    fields = record['fields']
    for field_name in LINKED_TABLES.keys():
        linked_ids = fields.get(field_name, [])
        if isinstance(linked_ids, list):
            readable_names = [linked_id_maps[field_name].get(id, 'Unknown') for id in linked_ids]
            fields[f"{field_name} (Name)"] = ", ".join(readable_names)

# Step 4: Write to CSV
output_file = 'OT2.csv'
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
