import requests
import csv
import os
from urllib.parse import quote
from datetime import datetime
import pandas as pd
import subprocess

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

# --- Helper: fetch all records from Airtable ---
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

# Step 1: Linked lookup maps
linked_id_maps = {}
for field, table_name in LINKED_TABLES.items():
    records = fetch_all_records(table_name)
    display_field = DISPLAY_FIELDS[field]
    id_to_display = {
        rec['id']: rec['fields'].get(display_field, 'Unknown')
        for rec in records
    }
    linked_id_maps[field] = id_to_display

# Step 2: Workshop Master records
workshop_master_records = fetch_all_records(WORKSHOP_MASTER_TABLE)
print("🔎 First 3 Workshop Master records (raw fields):")
for rec in workshop_master_records[:3]:
    print(rec['fields'])

workshop_master_map = {
    rec['fields'].get('Workshop', 'Unknown'): {
        "City": rec['fields'].get('Location', 'Unknown'),
        "# of Days": rec['fields'].get('# of days', 0),
        "Total Agenda Hours": rec['fields'].get('Total Agenda Hours', 0),
        "Fiscal Year": rec['fields'].get('Fiscal Year', 'Unknown')
    }
    for rec in workshop_master_records
}

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

    # Workshop & Workstream enrichment
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

    # Attach workshop master info by Workshop text
    workshop_name = fields.get("Workshop (Name)", "Unknown")
    wm_info = workshop_master_map.get(workshop_name, {})
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

# Step 5: Export enriched OT1
output_file = 'OT1.csv'
with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
    fieldnames = [
        'Indicator ID',
        'Workshop',
        'Workshop (Name)',
        'Workshop Date',
        'Email Address',
        'Sex',
        'Economy',
        'Economy (Name)',
        'Fiscal Year',
        'Other Economy',
        'Organization',
        'Workstream',
        'Workstream (Name)',
        'Sector',
        'Sector (Name)',
        'Workshop City',
        '# of Days',
        'Total Agenda Hours',
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

# Step 6: Person-Hours calculations
ot1_df = pd.read_csv("OT1.csv")
ot1_df["Workshop Date"] = pd.to_datetime(ot1_df["Workshop Date"], errors="coerce")

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

ot1_df["Fiscal Quarter"] = ot1_df["Workshop Date"].apply(fiscal_quarter)

# Days attended per participant
attendance = (
    ot1_df.groupby(["Workshop (Name)", "Email Address"])["Workshop Date"]
    .nunique()
    .reset_index(name="Days Attended")
)
merged = pd.merge(
    ot1_df,
    attendance,
    on=["Workshop (Name)", "Email Address"],
    how="left"
)

# Full attendance & Person-Hours
merged["Full Attendance Flag"] = (merged["Days Attended"] == merged["# of Days"]).astype(int)
merged["Person-Hours"] = merged["Full Attendance Flag"] * merged["Total Agenda Hours"]

# Step 7: Disaggregated export
tidy = (
    merged.groupby(
        ["Fiscal Year", "Fiscal Quarter", "Economy (Name)", "Workshop City",
         "Workshop (Name)", "Sex", "Sector (Name)", "Workstream (Name)"],
        dropna=False
    )["Person-Hours"].sum().reset_index()
)
tidy.rename(columns={
    "Economy (Name)": "Economy",
    "Workshop City": "City",
    "Workshop (Name)": "Workshop",
    "Sector (Name)": "Sector",
    "Workstream (Name)": "Workstream"
}, inplace=True)
tidy.to_csv("person_hours.csv", index=False)
print("✅ Export complete: person_hours.csv")

# Step 8: Workshop-level totals
workshop_totals = (
    merged.groupby(
        ["Fiscal Year", "Fiscal Quarter", "Workshop (Name)", "Economy (Name)", "Workshop City"],
        dropna=False
    )["Person-Hours"].sum().reset_index()
)
workshop_totals.rename(columns={
    "Workshop (Name)": "Workshop",
    "Economy (Name)": "Economy",
    "Workshop City": "City"
}, inplace=True)
workshop_totals.to_csv("person_hours_by_workshop.csv", index=False)
print("✅ Export complete: person_hours_by_workshop.csv")

# Step 9: Totals summary
fy_totals = workshop_totals.groupby("Fiscal Year")["Person-Hours"].sum().reset_index()
fyq_totals = workshop_totals.groupby(["Fiscal Year", "Fiscal Quarter"])["Person-Hours"].sum().reset_index()
grand_total = workshop_totals["Person-Hours"].sum()

print("🔎 Totals summary:")
print("By FY:\n", fy_totals)
print("By FY+Quarter:\n", fyq_totals)
print("Grand Total:", grand_total)

# Step 10: Sex-level totals
sex_totals = (
    merged.groupby(["Fiscal Year", "Fiscal Quarter", "Sex"])["Person-Hours"]
    .sum().reset_index()
)
sex_totals.to_csv("person_hours_by_sex.csv", index=False)
print("✅ Export complete: person_hours_by_sex.csv")

# Step 11: Sector-level totals
sector_totals = (
    merged.groupby(["Fiscal Year", "Fiscal Quarter", "Sector (Name)"])["Person-Hours"]
    .sum().reset_index()
)
sector_totals.rename(columns={"Sector (Name)": "Sector"}, inplace=True)
sector_totals.to_csv("person_hours_by_sector.csv", index=False)
print("✅ Export complete: person_hours_by_sector.csv")

# Step 12: Economy-level totals
economy_totals = (
    merged.groupby(["Fiscal Year", "Fiscal Quarter", "Economy (Name)"])["Person-Hours"]
    .sum().reset_index()
)
economy_totals.rename(columns={"Economy (Name)": "Economy"}, inplace=True)
economy_totals.to_csv("person_hours_by_economy.csv", index=False)
print("✅ Export complete: person_hours_by_economy.csv")

# Step 13: Combined totals for Tableau
combined_totals = []

# Fiscal Year totals
for _, row in fy_totals.iterrows():
    combined_totals.append({
        "Dimension": "Fiscal Year",
        "Category": row["Fiscal Year"],
        "Person-Hours": row["Person-Hours"]
    })

# Fiscal Year + Quarter totals
for _, row in fyq_totals.iterrows():
    combined_totals.append({
        "Dimension": "Fiscal Quarter",
        "Category": f"{row['Fiscal Year']} - {row['Fiscal Quarter']}",
        "Person-Hours": row["Person-Hours"]
    })

# Sex totals
for _, row in sex_totals.iterrows():
    combined_totals.append({
        "Dimension": "Sex",
        "Category": row["Sex"],
        "Person-Hours": row["Person-Hours"]
    })

# Sector totals
for _, row in sector_totals.iterrows():
    combined_totals.append({
        "Dimension": "Sector",
        "Category": row["Sector"],
        "Person-Hours": row["Person-Hours"]
    })

# Economy totals
for _, row in economy_totals.iterrows():
    combined_totals.append({
        "Dimension": "Economy",
        "Category": row["Economy"],
        "Person-Hours": row["Person-Hours"]
    })

# Grand Total
combined_totals.append({
    "Dimension": "Overall",
    "Category": "Grand Total",
    "Person-Hours": grand_total
})

combined_df = pd.DataFrame(combined_totals)
combined_df.to_csv("person_hours_totals_combined.csv", index=False)
print("✅ Export complete: person_hours_totals_combined.csv")
print("🔎 Combined Totals Preview:\n", combined_df.head(10))

# Step 14: Push outputs to GitHub
def git_push(files, message="Update OT1 person-hours exports"):
    try:
        subprocess.run(["git", "add"] + files, check=True)
        subprocess.run(["git", "commit", "-m", message], check=True)
        subprocess.run(["git", "push"], check=True)
        print(f"✅ Successfully pushed: {files}")
    except subprocess.CalledProcessError as e:
        print(f"❌ Git push failed: {e}")

files_to_push = [
    "OT1.csv",
    "person_hours.csv",
    "person_hours_by_workshop.csv",
    "person_hours_by_sex.csv",
    "person_hours_by_sector.csv",
    "person_hours_by_economy.csv",
    "person_hours_totals_combined.csv"
]
git_push(files_to_push)
