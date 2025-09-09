import requests
import csv
import os
from urllib.parse import quote
from datetime import datetime

# Airtable credentials
AIRTABLE_TOKEN = os.environ['AIRTABLE_TOKEN']
BASE_ID = 'app0Ljjhrp3lTTpTO'
MAIN_TABLE = 'Map Data'   # your main table
VIEW_NAME = 'Grid view'   # or None if you want all records

# Linked table configs
LINKED_TABLES = {
    'Economy': 'Economy Reference List',
    'Workstream': 'Workstream Reference List'
}

# Fields to pull from linked tables
DISPLAY_FIELDS = {
    'Economy': 'Economy',
    'Workstream': 'Workstream'
}

headers = {"Authorization": f"Bearer {AIRTABLE_TOKEN}"}

# --- Helper: fetch all records from a table ---
def fetch_all_records(table, view=None):
    url = f"https://api.airtable.com/v0/{BASE_ID}/{quote(table)}"
    if view:
        url += f"?view={quote(view)}"
    all_records, offset = [], None

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

if __name__ == "__main__":
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

    # Step 2: Fetch main Map Data records
    main_records = fetch_all_records(MAIN_TABLE, view=VIEW_NAME)
    print(f"🔍 Retrieved {len(main_records)} records from {MAIN_TABLE}")

    # Step 3: Resolve linked IDs → names, add metrics
    timestamp = datetime.utcnow().isoformat()
    for record in main_records:
        fields = record['fields']
        for field_name in LINKED_TABLES.keys():
            linked_ids = fields.get(field_name, [])
            if isinstance(linked_ids, list):
                readable_names = [
                    linked_id_maps[field_name].get(id, 'Unknown')
                    for id in linked_ids
                ]
                fields[f"{field_name} (Name)"] = ", ".join(readable_names)
                fields[f"{field_name} Count"] = len(readable_names)
        fields['Activity Count'] = 1
        fields['Last Updated'] = timestamp

    # Step 4: Export to CSV
    output_file = 'Map_Data.csv'
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
