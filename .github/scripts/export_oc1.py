import requests
import csv
import os
from urllib.parse import quote

# Airtable credentials and config
AIRTABLE_TOKEN = os.environ['AIRTABLE_TOKEN']
BASE_ID = 'app0Ljjhrp3lTTpTO'
MAIN_TABLE = 'OC1 Policy Reforms'
VIEW_NAME = 'Grid view'

# Linked table names
LINKED_TABLES = {
    'Economy': 'Economy Reference List',
    'Workstream': 'Workstream Reference List'
}

# Fields to display from the linked tables
DISPLAY_FIELDS = {
    'Economy': 'Economy Name',
    'Workstream': 'Workstream Name'
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

# Step 1: Fetch linked records and build lookup dictionaries
linked_id_maps = {}
for field, table_name in LINKED_TABLES.items():
    records = fetch_all_records(table_name)
    display_field = DISPLAY_FIELDS[field]
    id_to_display = {
        rec['id']: rec['fields'].get(display_field, 'Unknown')
        for rec in records
    }
    linked_id_maps[field] = id_to_display

# Step 2: Fetch main table records
main_records = fetch_all_records(MAIN_TABLE, view=VIEW_NAME)

# Step 3: Replace linked record IDs with display names
for record in main_records:
    fields = record['fields']
    for field_name in LINKED_TABLES.keys():
        linked_value = fields.get(field_name, [])
        if isinstance(linked_value, list):
            readable_names = [linked_id_maps[field_name].get(id, 'Unknown') for id in linked_value]
            fields[f"{field_name} (Name)"] = ", ".join(readable_names)
        elif isinstance(linked_value, str):
            # Just in case it's a single string instead of a list
            fields[f"{field_name} (Name)"] = linked_id_maps[field_name].get(linked_value, 'Unknown')
