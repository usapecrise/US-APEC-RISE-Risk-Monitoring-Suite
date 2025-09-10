import requests
import csv
import os
from urllib.parse import quote
from datetime import datetime

# Airtable credentials
AIRTABLE_TOKEN = os.environ['AIRTABLE_TOKEN']
BASE_ID = 'app0Ljjhrp3lTTpTO'
MAIN_TABLE = 'Map Data'
VIEW_NAME = 'Grid view'

# Linked tables (key = field name in Map Data, value = table name in Airtable)
LINKED_TABLES = {
    'Economy': 'Economy Reference List',
    'Workstream': 'Workstream Reference List'
}

# Which field to display from each linked table
DISPLAY_FIELDS = {
    'Economy': 'Economy',
    'Workstream': 'Workstream'
}

headers = {"Authorization": f"Bearer {AIRTABLE_TOKEN}"}

def fetch_all_records(table, view=None):
    """Fetch all records from an Airtable table"""
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

    # Step 2: Fetch Map Data records
    main_records = fetch_all_records(MAIN_TABLE, view=VIEW_NAME)
    print(f"🔍 Retrieved {len(main_records)} records from {MAIN_TABLE}")

    # Step 3: Resolve linked IDs → names, add helper fields
    timestamp = datetime.utcnow().isoformat()
    for record in main_records:
        fields = record['fields']

        # Flatten linked fields
        for field_name in LINKED_TABLES.keys():
            linked_ids = fields.get(field_name, [])
            if isinstance(linked_ids, list):
                readable_names = [
                    linked_id_maps[field_name].get(id, 'Unknown')
                    for id in linked_ids
                ]
                fields[f"{field_name} (Name)"] = ", ".join(readable_names)
                fields[f"{field_name} Count"] = len(readable_names)

        # Ensure Start Date is always present
        fields['Start Date'] = fields.get('Start Date', None)

        # --- Add Metric Value ---
        if 'Participants' in fields and isinstance(fields['Participants'], (int, float)):
            fields['Metric Value'] = fields['Participants']
        elif '# Participants' in fields and isinstance(fields['# Participants'], (int, float)):
            fields['Metric Value'] = fields['# Participants']
        elif '# Outputs Delivered' in fields and isinstance(fields['# Outputs Delivered'], (int, float)):
            fields['Metric Value'] = fields['# Outputs Delivered']
        else:
            fields['Metric Value'] = 1  # default

        fields['Activity Count'] = 1
        fields['Last Updated'] = timestamp

    # Step 4: Define expected fields (fixed schema for Tableau)
    expected_fields = [
        "Activity Title", "Activity Type",
        "Economy (Name)", "Economy Count",
        "Workstream (Name)", "Workstream Count",
        "City", "Latitude", "Longitude",
        "Start Date",
        "# Participants", "# Outputs Delivered",
        "Metric Value", "Activity Count",
        "Last Updated", "Deliverable / Notes"
    ]

    # Step 5: Export to CSV with fixed headers
    output_file = 'Map_Data.csv'
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=expected_fields)
        writer.writeheader()
        for rec in main_records:
            row = {}
            for f in expected_fields:
                row[f] = rec['fields'].get(f, "")
            writer.writerow(row)

    print(f"✅ Export complete: {output_file}")

