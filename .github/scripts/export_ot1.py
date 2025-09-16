import requests
import csv
import os
from urllib.parse import quote
from datetime import datetime
import pandas as pd

# Airtable credentials
AIRTABLE_TOKEN = os.environ['AIRTABLE_TOKEN']
BASE_ID = 'app0Ljjhrp3lTTpTO'
MAIN_TABLE = 'OT1 Sign-Ins (Workshops)'
WORKSHOP_MASTER_TABLE = 'Workshop Reference List'
VIEW_NAME = 'Grid view'

# Linked tables
LINKED_TABLES = {
    'Workstream': 'Workstream Reference List',
    'Economy': 'Economy Reference List'
}
DISPLAY_FIELDS = {
    'Workstream': 'Workstream',
    'Economy': 'Economy'
}
headers = {"Authorization": f"Bearer {AIRTABLE_TOKEN}"}

# ---------------------- Helpers ----------------------
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

def flatten(value):
    if isinstance(value, list):
        return ", ".join(str(v) for v in value)
    if value in [None, ""]:
        return "Unknown"
    return str(value)

# ---------------------- Step 1: Lookups ----------------------
linked_id_maps = {}
for field, table_name in LINKED_TABLES.items():
    records = fetch_all_records(table_name)
    display_field = DISPLAY_FIELDS[field]
    id_to_display = {rec['id']: rec['fields'].get(display_field, 'Unknown') for rec in records}
    linked_id_maps[field] = id_to_display

# Workshop master records (keyed by record ID)
workshop_master_records = fetch_all_records(WORKSHOP_MASTER_TABLE)
workshop_master_map = {
    rec["id"]: {
        "Workshop": rec["fields"].get("Workshop", "Unknown"),
        "City": rec["fields"].get("City", "Unknown"),
        "# of days": rec["fields"].get("# of days", 0),
        "Total Agenda Hours": rec["fields"].get("Total Agenda Hours", 0),
        "Fiscal Year": rec["fields"].get("Fiscal Year", "Unknown"),
    }
    for rec in workshop_master_records
}
print(f"🔎 Example Workshop Master Keys: {list(workshop_master_map.keys())[:3]}")

# ---------------------- Step 2: OT1 Records ----------------------
main_records = fetch_all_records(MAIN_TABLE, view=VIEW_NAME)
print(f"🔍 Retrieved {len(main_records)} records from {MAIN_TABLE}")

timestamp = datetime.utcnow().isoformat()
for record in main_records:
    fields = record['fields']

    # Economy enrichment
    economy_ids = fields.get('Economy') or fields.get('Guest Economy') or []
    if isinstance(economy_ids, str):
        economy_ids = [economy_ids]
    if isinstance(economy_ids, list) and economy_ids:
        readable_econ = [linked_id_maps['Economy'].get(eid, 'Unknown') for eid in economy_ids]
        fields['Economy (Name)'] = ", ".join(readable_econ)
    else:
        fields['Economy (Name)'] = "Unknown"

    # Workstream enrichment
    workstream_ids = fields.get("Workstream", [])
    if isinstance(workstream_ids, str):
        workstream_ids = [workstream_ids]
    readable_ws = [linked_id_maps['Workstream'].get(wid, 'Unknown') for wid in workstream_ids] if workstream_ids else []
    fields["Workstream (Name)"] = ", ".join(readable_ws) if readable_ws else "Unknown"

    # Attach Workshop Master info (using linked record ID)
    workshop_ids = fields.get("Workshop", [])
    if isinstance(workshop_ids, str):
        workshop_ids = [workshop_ids]
    if workshop_ids:
        wm_info = workshop_master_map.get(workshop_ids[0], {})
        fields['Workshop (Name)'] = wm_info.get("Workshop", "Unknown")
        fields['Workshop City'] = wm_info.get("City", "Unknown")
        fields['# of days'] = wm_info.get("# of days", 0)
        fields['Total Agenda Hours'] = wm_info.get("Total Agenda Hours", 0)
        fields['Fiscal Year'] = wm_info.get("Fiscal Year", "Unknown")
    else:
        fields['Workshop (Name)'] = "Unknown"
        fields['Workshop City'] = "Unknown"
        fields['# of days'] = 0
        fields['Total Agenda Hours'] = 0
        fields['Fiscal Year'] = "Unknown"

    # Sector
    sector_vals = fields.get('Sector', [])
    if isinstance(sector_vals, str):
        sector_vals = [sector_vals]
    elif not isinstance(sector_vals, list):
        sector_vals = []
    fields['Sector (Name)'] = ", ".join(sector_vals) if sector_vals else "Unknown"

    fields['Last Updated'] = timestamp
    fields['Indicator ID'] = 'OT1'

# ---------------------- Step 3: Export OT1 ----------------------
output_file = 'OT1.csv'
with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
    fieldnames = [
        'Indicator ID','Workshop','Workshop (Name)','Workshop Date','Email Address','Sex',
        'Economy','Economy (Name)','Fiscal Year','Other Economy','Organization','Workstream',
        'Workstream (Name)','Sector','Sector (Name)','Workshop City','# of days',
        'Total Agenda Hours','Last Updated'
    ]
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for rec in main_records:
        row = rec['fields']
        writer.writerow({key: flatten(row.get(key)) for key in fieldnames})
print(f"✅ Export complete: {output_file}")

# ---------------------- Step 4: Person-Hours ----------------------
ot1_df = pd.read_csv("OT1.csv")
ot1_df["Workshop Date"] = pd.to_datetime(ot1_df["Workshop Date"], errors="coerce")

# Fiscal quarter (US FY Oct–Sep)
def fiscal_quarter(date):
    if pd.isna(date): return "Unknown"
    month, year = date.month, date.year
    if month >= 10: fy, q = year + 1, "Q1"
    elif month >= 7: fy, q = year, "Q4"
    elif month >= 4: fy, q = year, "Q3"
    else: fy, q = year, "Q2"
    return f"FY{fy}-{q}"
ot1_df["Fiscal Quarter"] = ot1_df["Workshop Date"].apply(fiscal_quarter)

attendance = (
    ot1_df.groupby(["Workshop","Email Address"])["Workshop Date"]
    .nunique().reset_index(name="Days Attended")
)
merged = pd.merge(ot1_df, attendance, on=["Workshop","Email Address"], how="left")
merged["Full Attendance Flag"] = (merged["Days Attended"] == merged["# of days"]).astype(int)
merged["Person-Hours"] = merged["Full Attendance Flag"] * merged["Total Agenda Hours"]

# ---------------------- Step 5: Outputs ----------------------
# Tidy disaggregation
tidy = (
    merged.groupby(
        ["Fiscal Year","Fiscal Quarter","Economy (Name)","Workshop City",
         "Workshop (Name)","Sex","Sector (Name)","Workstream (Name)"],
        dropna=False
    )["Person-Hours"].sum().reset_index()
)
tidy.rename(columns={
    "Economy (Name)":"Economy","Workshop City":"City","Workshop (Name)":"Workshop",
    "Sector (Name)":"Sector","Workstream (Name)":"Workstream"
}, inplace=True)
tidy.to_csv("person_hours.csv", index=False)
print("✅ Export complete: person_hours.csv")

# By Workshop
workshop_totals = (
    merged.groupby(["Fiscal Year","Fiscal Quarter","Workshop (Name)","Economy (Name)","Workshop City"])
    ["Person-Hours"].sum().reset_index()
)
workshop_totals.rename(columns={
    "Workshop (Name)":"Workshop","Economy (Name)":"Economy","Workshop City":"City"
}, inplace=True)
workshop_totals.to_csv("person_hours_by_workshop.csv", index=False)
print("✅ Export complete: person_hours_by_workshop.csv")

# By Sex
merged.groupby("Sex")["Person-Hours"].sum().reset_index().to_csv("person_hours_by_sex.csv", index=False)
print("✅ Export complete: person_hours_by_sex.csv")

# By Sector
merged.groupby("Sector (Name)")["Person-Hours"].sum().reset_index().rename(
    columns={"Sector (Name)":"Sector"}).to_csv("person_hours_by_sector.csv", index=False)
print("✅ Export complete: person_hours_by_sector.csv")

# By Economy
merged.groupby("Economy (Name)")["Person-Hours"].sum().reset_index().rename(
    columns={"Economy (Name)":"Economy"}).to_csv("person_hours_by_economy.csv", index=False)
print("✅ Export complete: person_hours_by_economy.csv")

# Totals combined
grand_totals = []
fy_totals = merged.groupby("Fiscal Year")["Person-Hours"].sum().reset_index()
for _, row in fy_totals.iterrows():
    grand_totals.append({"Dimension":"Fiscal Year","Category":row["Fiscal Year"],"Person-Hours":row["Person-Hours"]})
fyq_totals = merged.groupby(["Fiscal Year","Fiscal Quarter"])["Person-Hours"].sum().reset_index()
for _, row in fyq_totals.iterrows():
    grand_totals.append({"Dimension":"Fiscal Quarter","Category":f"{row['Fiscal Year']} - {row['Fiscal Quarter']}","Person-Hours":row["Person-Hours"]})
sex_totals = merged.groupby("Sex")["Person-Hours"].sum().reset_index()
for _, row in sex_totals.iterrows():
    grand_totals.append({"Dimension":"Sex","Category":row["Sex"],"Person-Hours":row["Person-Hours"]})
sector_totals = merged.groupby("Sector (Name)")["Person-Hours"].sum().reset_index()
for _, row in sector_totals.iterrows():
    grand_totals.append({"Dimension":"Sector","Category":row["Sector (Name)"],"Person-Hours":row["Person-Hours"]})
econ_totals = merged.groupby("Economy (Name)")["Person-Hours"].sum().reset_index()
for _, row in econ_totals.iterrows():
    grand_totals.append({"Dimension":"Economy","Category":row["Economy (Name)"],"Person-Hours":row["Person-Hours"]})

grand_df = pd.DataFrame(grand_totals)
grand_df.to_csv("person_hours_totals_combined.csv", index=False)
print("✅ Export complete: person_hours_totals_combined.csv")
print("🔎 Combined Totals Preview:")
print(grand_df.head(10))
