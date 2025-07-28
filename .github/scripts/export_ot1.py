import requests
import csv
import os
from urllib.parse import quote
from datetime import datetime

# Airtable credentials and config
AIRTABLE_TOKEN = os.environ['AIRTABLE_TOKEN']
BASE_ID = 'app0Ljjhrp3lTTpTO'
MAIN_TABLE = 'OT1 Sign-Ins (Workshops)'
VIEW_NAME = 'Grid view'

# Linked table names and display fields
LINKED_TABLES = {
    'Workstream': 'Workstream Reference List',
    'Workshop': 'Workshop Reference List',
    'Economy': 'Economy Reference List'  # Used for both 'Economy' and 'Guest Economy'
}

DISPLAY_FIELDS = {
    'Workstream': 'Workstream',
    'Workshop': 'Workshop',  # Change to 'Title' if your Workshop table uses a different field
    'Economy': 'Economy'
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

# Step 2: Fetch OT1 Sign-In records
main_records = fetch_all_records(MAIN_TABLE, view=VIEW_NAME)
print(f"🔍 Retrieved {len(main_records)} records from {MAIN_TABLE}")

# Step 3: Enrich each record with readable linked values and timestamp
timestamp = datetime.utcnow().isoformat()
for record in main_records:
    fields = record['fields']

    # Custom: Handle Economy or Guest Economy fallback
    economy_ids = fields.get('Economy') or fields.get('Guest Economy') or []
    if isinstance(economy_ids, str):
        economy_ids = [economy_ids]
    if isinstance(economy_ids, list) and economy_ids:
        readable_economies = [linked_id_maps['Economy'].get(eid, 'Unknown') for eid in economy_ids]
        fields['Economy (Name)'] = ", ".join(readable_economies)
    else:
        fields['Economy (Name)'] = "Unknown"

    # Enrich other linked fields
    for field_name in ['Workshop', 'Workstream']:
        raw_value = fields.get(field_name)
        if isinstance(raw_value, str):
            linked_ids = [raw_value]
        elif isinstance(raw_value, list):
            linked_ids = raw_value
        else:
            linked_ids = []

        if linked_ids:
            readable_names = [linked_id_maps[field_name].get(id, 'Unknown') for id in linked_ids]
            fields[f"{field_name} (Name)"] = ", ".join(readable_names)
        else:
            fields[f"{field_name} (Name)"] = "Unknown"

    fields['Last Updated'] = timestamp

# Step 4: Export to CSV
output_file = 'OT1.csv'
with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
    fieldnames = [
        'Indicator ID',
        'Workshop',
        'Workshop (Name)',
        'Workshop Date',
        'Sex',
        'Economy',
        'Economy (Name)',
        'Fiscal Year',
        'Other Economy',
        'Organization',
        'Workstream',
        'Workstream (Name)',
        'Last Updated'
    ]

    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    if main_records:
        for rec in main_records:
            row = rec['fields']
            row['Indicator ID'] = 'OT1'  # Optional: static value for tracking
            filtered_row = {key: row.get(key, '') for key in fieldnames}
            writer.writerow(filtered_row)
    else:
        print("⚠️ No OT1 records found — writing header only.")

print(f"✅ Export complete: {output_file}")

