import requests
import csv
import os
import pandas as pd
from urllib.parse import quote
from datetime import datetime
from itertools import product

# Airtable credentials and config
AIRTABLE_TOKEN = os.environ['AIRTABLE_TOKEN']
BASE_ID = 'app0Ljjhrp3lTTpTO'
MAIN_TABLE = 'OT4 Private Sector Firms'
VIEW_NAME = 'Grid view'

# Linked table names
LINKED_TABLES = {
    'Economy': 'Economy Reference List',
    'Workstream': 'Workstream Reference List',
}

# Display field from each linked table
DISPLAY_FIELDS = {
    'Economy': 'Economy',
    'Workstream': 'Workstream',
}

headers = {"Authorization": f"Bearer {AIRTABLE_TOKEN}"}

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

# Step 1: Build lookup maps
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

# Step 3: Process and normalize data
flattened_rows = []
for record in main_records:
    fields = record.get('fields', {})

    firm_name = fields.get('Firm') or fields.get('Name') or fields.get('Organization Name') or 'Unknown'

    economy = [
        linked_id_maps['Economy'].get(eid, 'Unknown')
        for eid in fields.get('Economy', [])
        if eid in linked_id_maps['Economy']
    ] or ['Unknown']

    workstreams = [
        linked_id_maps['Workstream'].get(wid, 'Unknown')
        for wid in fields.get('Workstream', [])
        if wid in linked_id_maps['Workstream']
    ] or ['Unknown']

    fiscal_years = fields.get('Fiscal Year', [])
    fiscal_years = fiscal_years if isinstance(fiscal_years, list) and fiscal_years else ['Unknown']

    pse_origin = fields.get('PSE Origin', '')
    pse_size = fields.get('PSE Size', '')
    pse_type = fields.get('PSE Type', '')

    for combo in product(workstreams, fiscal_years):
        flattened_rows.append({
            'Firm (Name)': firm_name,
            'Economy (Name)': ', '.join(economy),
            'Workstream (Name)': combo[0],
            'Fiscal Year': combo[1],
            'PSE Origin': pse_origin,
            'PSE Size': pse_size,
            'PSE Type': pse_type,
            'Timestamp': datetime.utcnow().isoformat() + "Z"
        })

# Step 4: Export to final CSV
output_file = 'OT4.csv'
EXPORT_FIELDS = [
    'Firm (Name)',
    'Economy (Name)',
    'Workstream (Name)',
    'Fiscal Year',
    'PSE Origin',
    'PSE Size',
    'PSE Type',
    'Timestamp'
]

df = pd.DataFrame(flattened_rows)
df.to_csv(output_file, index=False, columns=EXPORT_FIELDS)
print(f"✅ Final deduplicated and exploded file created: {output_file}")
