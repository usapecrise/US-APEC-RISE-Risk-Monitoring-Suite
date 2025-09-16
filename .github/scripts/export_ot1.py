import requests
import csv
import os
from urllib.parse import quote
from datetime import datetime
import pandas as pd

# Airtable credentials and config
AIRTABLE_TOKEN = os.environ['AIRTABLE_TOKEN']
BASE_ID = 'app0Ljjhrp3lTTpTO'
MAIN_TABLE = 'OT1 Sign-Ins (Workshops)'
WORKSHOP_MASTER_TABLE = 'Workshop Reference List'
VIEW_NAME = 'Grid view'

# Linked table names and display fields
LINKED_TABLES = {
    'Workstream': 'Workstream Reference List',
    'Workshop': 'Workshop Reference List',
    'Economy': 'Economy Reference List'
}

DISPLAY_FIELDS = {
    'Workstream': 'Workstream',
    'Workshop': 'Workshop',
    'Economy': 'Economy'
}

headers = {"Authorization": f"Bearer {AIRTABLE_TOKEN}"}

# Helper: fetch all records from Airtable
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

# Normalize function for safe joins
def normalize(text):
    if not isinstance(text, str):
        return "unknown"
    return text.strip().lower()

# Step 1: Build lookup maps
linked_id_maps = {}
for field, table_name in LINKED_TABLES.items():
    records = fetch_all_records(table_name)
    display_field = DISPLAY_FIELDS[field]
    id_to_display = {
        rec['id']: rec['fields'].get(display_field, 'Unknown')
        for rec in records
    }
    linked_id_maps[field] = id_to_display

# Step 2: Workshop master records (normalized keys)
workshop_master_records = fetch_all_records(WORKSHOP_MASTER_TABLE)
workshop_master_map = {
    normalize(rec['fields'].get('Workshop', 'Unknown')): {
        "City": rec['fields'].get('City', 'Unknown'),
        "# of Days": rec['fields'].get('# of days', 0),  # lowercase d
        "Total Agenda Hours": rec['fields'].get('Total Agenda Hours', 0),
        "Fiscal Year": rec['fields'].get('Fiscal Year', 'Unknown')
    }
    for rec in workshop_master_records
}
print("🔎 Workshop Master keys (sample):", list(workshop_master_map.keys())[:10])

# Step 3: OT1 sign-in records
main_records = fetch_all_records(MAIN_TABLE, view=VIEW_NAME)
print(f"🔍 Retrieved {len(main_records)} records from {MAIN_TABLE}")

# Step 4: Enrich OT1 rows
timestamp = datetime.utcnow().isoformat()
for record in main_records:
    fields = record['fields']

    # Economy enrichment
    economy_ids = fields.get('Economy') or fields.get('Guest Economy') or []
    if isinstance(economy_ids, str):
        economy_ids = [economy_ids]
    if isinstance(economy_ids, list) and economy_ids:
        readable_economies = [linked_id_maps['Economy'].get(eid, 'Unknown') for eid in economy_ids]
        fields['Economy (Name)'] = ", ".join(readable_economies)
    else:
        fields['Economy (Name)'] = "Unknown"

    # Attach workshop master info using normalized Workshop text
    workshop_name = fields.get("Workshop", "Unknown")
    wm_info = workshop_master_map.get(normalize(workshop_name), {})
    fields['Workshop City'] = wm_info.get("City", "Unknown")
    fields['# of Days'] = wm_info.get("# of Days", 0)
    fields['Total Agenda Hours'] = wm_info.get("Total Agenda Hours", 0)
    fields['Fiscal Year'] = wm_info.get("Fiscal Year", "Unknown")

    # Sector enrichment
    sector_values = fields.get('Sector', [])
    if isinstance(sector_values, str):
        sector_values = [sector_values]
    elif not isinstance(sector_values, list):
        sector_values = []
    fields['Sector (Name)'] = ", ".join(sector_values) if sector_values else "Unknown"

    fields['Last Updated'] = timestamp
    fields['Indicator ID'] = 'OT1'

# Flatten helper
def flatten(value):
    if isinstance(value, list):
        return ", ".join(str(v) for v in value)
    if value in [None, ""]:
        return "Unknown"
    return str(value)

# Step 5: Export OT1.csv
output_file = 'OT1.csv'
with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
    fieldnames = [
        'Indicator ID',
        'Workshop',
        'Workshop City',
        '# of Days',
        'Total Agenda Hours',
        'Fiscal Year',
        'Last Updated'
    ]
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    if main_records:
        for rec in main_records:
            row = rec['fields']
            filtered_row = {key: flatten(row.get(key)) for key in fieldnames}
            writer.writerow(filtered_row)
print(f"✅ Export complete: {output_file}")

# Step 6: Calculate Person-Hours
ot1_df = pd.read_csv("OT1.csv")
ot1_df["Workshop Date"] = pd.to_datetime(ot1_df.get("Workshop Date"), errors="coerce")

# Fiscal Quarter (US FY: Oct–Sep)
def fiscal_quarter(date):
    if pd.isna(date):
        return "Unknown"
    month, year = date.month, date.year
    if month >= 10: fy, q = year + 1, "Q1"
    elif month >= 7: fy, q = year, "Q4"
    elif month >= 4: fy, q = year, "Q3"
    else: fy, q = year, "Q2"
    return f"FY{fy}-{q}"

if "Workshop Date" in ot1_df.columns:
    ot1_df["Fiscal Quarter"] = ot1_df["Workshop Date"].apply(fiscal_quarter)
else:
    ot1_df["Fiscal Quarter"] = "Unknown"

attendance = (
    ot1_df.groupby(["Workshop"])["Workshop"].count().reset_index(name="Attendance")
)

print("🔎 Sample of enriched OT1 data:")
print(ot1_df.head())
