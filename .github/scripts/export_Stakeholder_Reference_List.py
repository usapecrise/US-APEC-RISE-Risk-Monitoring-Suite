import requests
import csv
import os
from urllib.parse import quote
from datetime import datetime
from itertools import product

# Airtable credentials and config
AIRTABLE_TOKEN = os.environ['AIRTABLE_TOKEN']
BASE_ID = 'app0Ljjhrp3lTTpTO'
MAIN_TABLE = 'Stakeholder Reference List'
VIEW_NAME = 'Grid view'

# Linked table names
LINKED_TABLES = {
    'Economy': 'Economy Reference List',
    'Workstream': 'Workstream Reference List'
}

# Fields to display from the linked tables
DISPLAY_FIELDS = {
    'Economy': 'Economy',
    'Workstream': 'Workstream'
}

headers = {"Authorization": f"Bearer {AIRTABLE_TOKEN}"}

def fetch_all_records(table, view=None):
    url = f"https://api.airtable.com/v0/{BASE_ID}/{quote(table)}"
    params = {}
    if view:
        params['view'] = view

    all_records = []
    while True:
        resp = requests.get(url, headers=headers, params=params)
        data = resp.json()
        if 'records' not in data:
            raise RuntimeError(f"Error fetching {table}: {data}")

        all_records.extend(data['records'])
        offset = data.get('offset')
        if not offset:
            break
        params['offset'] = offset

    print(f"✅ Fetched {len(all_records)} records from '{table}'")
    return all_records

# 1) Build lookup dicts for linked tables
linked_id_maps = {}
for field, table_name in LINKED_TABLES.items():
    records = fetch_all_records(table_name)
    display_field = DISPLAY_FIELDS[field]
    id_to_display = {
        rec['id']: rec['fields'].get(display_field, 'Unknown')
        for rec in records
    }
    linked_id_maps[field] = id_to_display

# 2) Fetch main table
main_records = fetch_all_records(MAIN_TABLE, view=VIEW_NAME)
print(f"🔍 Retrieved {len(main_records)} records from {MAIN_TABLE}")

timestamp = datetime.utcnow().isoformat()

# Containers for outputs
wide_rows = []
long_rows = []

for rec in main_records:
    fields = dict(rec.get('fields', {}))  # shallow copy
    fields['Last Updated'] = timestamp  # force file change for pipelines

    # Resolve linked IDs to names, but keep both list and display versions.
    workstream_ids = fields.get('Workstream', [])
    economy_ids = fields.get('Economy', [])

    # Lists of readable names
    workstream_names = [linked_id_maps['Workstream'].get(_id, 'Unknown') for _id in workstream_ids] if isinstance(workstream_ids, list) else []
    economy_names = [linked_id_maps['Economy'].get(_id, 'Unknown') for _id in economy_ids] if isinstance(economy_ids, list) else []

    # Human-friendly comma-joined fields (like your current output)
    fields['Workstream (Name)'] = ", ".join(workstream_names) if workstream_names else ""
    fields['Economy (Name)'] = ", ".join(economy_names) if economy_names else ""

    # Also add pipe-joined list versions for clarity (no commas to confuse CSV)
    fields['Workstream_List'] = "|".join(workstream_names) if workstream_names else ""
    fields['Economy_List'] = "|".join(economy_names) if economy_names else ""

    # Preserve original ID arrays (optional—useful for debugging)
    # fields['Workstream_IDs'] = "|".join(workstream_ids) if isinstance(workstream_ids, list) else ""
    # fields['Economy_IDs'] = "|".join(economy_ids) if isinstance(economy_ids, list) else ""

    wide_rows.append(fields)

    # ---- Build LONG rows (normalize multi-selects) ----
    # If no workstreams/economies are set, still emit one row so counts don't drop.
    ws_vals = workstream_names if workstream_names else [""]
    ec_vals = economy_names if economy_names else [""]

    for ws, ec in product(ws_vals, ec_vals):
        long_row = dict(fields)  # copy base fields
        long_row['Workstream (Name)'] = ws
        long_row['Economy (Name)'] = ec
        # Optional: include a flat key for joins or grouping
        long_row['Workstream_Single'] = ws
        long_row['Economy_Single'] = ec
        long_rows.append(long_row)

# 3) Write wide CSV (same spirit as before)
wide_out = 'Stakeholder_Reference_List.csv'
if wide_rows:
    # Stable header order: put common fields first, then the rest
    preferred = [
        'Title', 'Organization Type', 'Fiscal Year',
        'Workstream (Name)', 'Economy (Name)', 'Workstream_List', 'Economy_List',
        'Last Updated'
    ]
    other_fields = sorted({k for r in wide_rows for k in r.keys()} - set(preferred))
    fieldnames = preferred + other_fields

    with open(wide_out, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)
        writer.writeheader()
        for r in wide_rows:
            writer.writerow(r)

print(f"✅ Export complete: {wide_out}")

# 4) Write LONG CSV (normalized)
long_out = 'Stakeholder_Reference_List_long.csv'
if long_rows:
    preferred_long = [
        'Title', 'Organization Type', 'Fiscal Year',
        'Workstream_Single', 'Economy_Single',
        'Workstream (Name)', 'Economy (Name)',
        'Workstream_List', 'Economy_List',
        'Last Updated'
    ]
    other_fields_long = sorted({k for r in long_rows for k in r.keys()} - set(preferred_long))
    fieldnames_long = preferred_long + other_fields_long

    with open(long_out, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames_long, quoting=csv.QUOTE_MINIMAL)
        writer.writeheader()
        for r in long_rows:
            writer.writerow(r)

print(f"✅ Export complete: {long_out}")
