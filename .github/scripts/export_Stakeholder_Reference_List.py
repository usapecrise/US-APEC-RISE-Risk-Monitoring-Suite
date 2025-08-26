import requests
import csv
import os
from urllib.parse import quote
from datetime import datetime
from itertools import product

# --- Config ---
AIRTABLE_TOKEN = os.environ['AIRTABLE_TOKEN']
BASE_ID = 'app0Ljjhrp3lTTpTO'
MAIN_TABLE = 'Stakeholder Reference List'
VIEW_NAME = 'Grid view'

# Linked table names (Airtable table names)
LINKED_TABLES = {
    'Economy': 'Economy Reference List',
    'Workstream': 'Workstream Reference List',
}

# The display field to read from the linked tables
DISPLAY_FIELDS = {
    'Economy': 'Economy',
    'Workstream': 'Workstream',
}

# Which linked fields should be normalized (exploded) in the long export
NORMALIZE_FIELDS = ['Workstream', 'Economy']

WIDE_OUT = 'Stakeholder_Reference_List.csv'
LONG_OUT = 'Stakeholder_Reference_List_long.csv'

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

# 1) Build lookup dicts for linked tables: {airtable_record_id: display_value}
linked_id_maps = {}
for field, table_name in LINKED_TABLES.items():
    records = fetch_all_records(table_name)
    display_field = DISPLAY_FIELDS[field]
    id_to_display = {}
    for rec in records:
        rec_id = rec['id']
        display_val = rec.get('fields', {}).get(display_field, 'Unknown')
        id_to_display[rec_id] = display_val
    linked_id_maps[field] = id_to_display

# 2) Fetch main table
main_records = fetch_all_records(MAIN_TABLE, view=VIEW_NAME)
print(f"🔍 Retrieved {len(main_records)} records from {MAIN_TABLE}")

timestamp = datetime.utcnow().isoformat()

wide_rows = []
long_rows = []

for rec in main_records:
    fields = dict(rec.get('fields', {}))  # shallow copy
    # Ensure consistent keys exist
    for k in LINKED_TABLES.keys():
        if k not in fields:
            fields[k] = []

    # Resolve linked IDs -> names
    workstream_ids = fields.get('Workstream', [])
    economy_ids = fields.get('Economy', [])

    # Convert to lists if not already (Airtable returns list for linked fields)
    workstream_ids = workstream_ids if isinstance(workstream_ids, list) else []
    economy_ids = economy_ids if isinstance(economy_ids, list) else []

    workstream_names = [linked_id_maps['Workstream'].get(_id, 'Unknown') for _id in workstream_ids]
    economy_names = [linked_id_maps['Economy'].get(_id, 'Unknown') for _id in economy_ids]

    # Human-friendly comma-joined
    fields['Workstream (Name)'] = ", ".join(workstream_names) if workstream_names else ""
    fields['Economy (Name)'] = ", ".join(economy_names) if economy_names else ""

    # Optional pipe-joined list forms (no commas to confuse CSVs)
    fields['Workstream_List'] = "|".join(workstream_names) if workstream_names else ""
    fields['Economy_List'] = "|".join(economy_names) if economy_names else ""

    fields['Last Updated'] = timestamp  # bump for pipeline runs

    wide_rows.append(fields)

    # ---- Build LONG rows (normalize) ----
    # For each record, create one row per Workstream x Economy combination.
    # If either list is empty, use [""] so we still emit a row.
    ws_vals = workstream_names if workstream_names else [""]
    ec_vals = economy_names if economy_names else [""]

    for ws, ec in product(ws_vals, ec_vals):
        long_row = dict(fields)  # copy base
        long_row['Workstream (Name)'] = ws
        long_row['Economy (Name)'] = ec
        # single-value helpers for Tableau
        long_row['Workstream_Single'] = ws
        long_row['Economy_Single'] = ec
        long_rows.append(long_row)

# 3) Write wide CSV
if wide_rows:
    preferred = [
        'Title', 'Organization Type', 'Fiscal Year',
        'Workstream (Name)', 'Economy (Name)',
        'Workstream_List', 'Economy_List',
        'Last Updated'
    ]
    other_fields = sorted({k for r in wide_rows for k in r.keys()} - set(preferred))
    fieldnames = preferred + other_fields

    with open(WIDE_OUT, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)
        writer.writeheader()
        for r in wide_rows:
            writer.writerow(r)

print(f"✅ Export complete: {WIDE_OUT}")

# 4) Write long CSV
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

    with open(LONG_OUT, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames_long, quoting=csv.QUOTE_MINIMAL)
        writer.writeheader()
        for r in long_rows:
            writer.writerow(r)

print(f"✅ Export complete: {LONG_OUT}")

# 5) (Optional) Sanity checks
try:
    # Stakeholder titles should not disappear in long file
    wide_titles = {r.get('Title', '') for r in wide_rows}
    long_titles = {r.get('Title', '') for r in long_rows}
    missing = wide_titles - long_titles
    if missing:
        raise AssertionError(f"Titles missing in long export: {sorted(list(missing))[:5]} ...")
    print("🧪 Sanity check passed.")
except Exception as e:
    print(f"⚠️ Sanity check warning: {e}")
