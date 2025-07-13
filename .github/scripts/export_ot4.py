import requests
import csv
import os
import pandas as pd
from urllib.parse import quote
from datetime import datetime

# Airtable credentials and config
AIRTABLE_TOKEN = os.environ['AIRTABLE_TOKEN']
BASE_ID = 'app0Ljjhrp3lTTpTO'
MAIN_TABLE = 'OT4 Private Sector Firms'
VIEW_NAME = 'Grid view'

# Linked table names
LINKED_TABLES = {
    'Economy': 'Economy Reference List',
    'Workstream': 'Workstream Reference List',
    'Engagement': 'OT2 Private Sector Engagements',
    'Resource': 'OT5 Private Sector Resources'
}

# Display field from each linked table
DISPLAY_FIELDS = {
    'Economy': 'Economy',
    'Workstream': 'Workstream',
    'Engagement': 'Engagement',
    'Resource': 'Resource'
}

headers = {"Authorization": f"Bearer {AIRTABLE_TOKEN}"}

# Fetch all records from Airtable
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

    print(f"✅ Fetched {len(all_records)} from {table}")
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

# Step 2: Fetch main OT4 records
main_records = fetch_all_records(MAIN_TABLE, view=VIEW_NAME)

# Step 3: Resolve linked record IDs to display names and flatten fields
export_rows = []
for record in main_records:
    fields = record['fields']

    # Resolve linked fields to readable names
    for field_name in ['Workstream', 'Engagement', 'Economy', 'Resource']:
        linked_ids = fields.get(field_name, [])
        if isinstance(linked_ids, list):
            readable_names = [linked_id_maps[field_name].get(id, 'Unknown') for id in linked_ids]
            fields[f"{field_name} (Name)"] = ", ".join(readable_names)

    # Flatten Fiscal Year
    fiscal_year = fields.get('Fiscal Year', [])
    fields['Fiscal Year'] = ", ".join(fiscal_year) if isinstance(fiscal_year, list) else fiscal_year

    # Firm name (plain text)
    firm_name = fields.get('Firm') or fields.get('Name') or fields.get('Organization Name')
    fields['Firm (Name)'] = firm_name if firm_name else 'Unknown'

    export_rows.append(fields)

# Step 4: Write intermediate CSV
intermediate_file = 'OT4_clean.csv'
EXPORT_FIELDS = [
    'Firm (Name)',
    'Economy (Name)',
    'Workstream (Name)',
    'Engagement (Name)',
    'Fiscal Year',
    'PSE Origin',
    'PSE Size',
    'PSE Type'
]

with open(intermediate_file, 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=EXPORT_FIELDS)
    writer.writeheader()
    for row in export_rows:
        writer.writerow({field: row.get(field, '') for field in EXPORT_FIELDS})

print(f"✅ Clean file created: {intermediate_file}")

# Step 5: Explode multi-value fields and add timestamp
df = pd.read_csv(intermediate_file)

# Clean and split multi-select fields
multi_fields = ['Workstream (Name)', 'Engagement (Name)', 'Fiscal Year']
for field in multi_fields:
    df[field] = (
        df[field]
        .astype(str)
        .str.replace(r"[\[\]']", "", regex=True)
        .str.split(',')
    )

# Explode each multi-value field and strip whitespace
for field in multi_fields:
    df = df.explode(field)
    df[field] = df[field].str.strip()

# Add Timestamp column to each row
timestamp = datetime.utcnow().isoformat() + "Z"
df['Timestamp'] = timestamp

# Final export
final_file = 'OT4.csv'
df.to_csv(final_file, index=False)
print(f"✅ Final exploded and Tableau-ready file created: {final_file}")
