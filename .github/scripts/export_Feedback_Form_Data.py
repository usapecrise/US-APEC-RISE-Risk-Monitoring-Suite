import requests
import csv
import os
from urllib.parse import quote
from datetime import datetime
import pandas as pd  # NEW: to handle pivot

# Airtable credentials and config
AIRTABLE_TOKEN = os.environ['AIRTABLE_TOKEN']
BASE_ID        = 'app0Ljjhrp3lTTpTO'
MAIN_TABLE     = 'Feedback Form Entries'
VIEW_NAME      = 'Grid view'

headers = {"Authorization": f"Bearer {AIRTABLE_TOKEN}"}

def fetch_all_records(table, view=None):
    url = f"https://api.airtable.com/v0/{BASE_ID}/{quote(table)}"
    params, all_records, offset = ({}, [], None)

    if view:
        params['view'] = view

    while True:
        if offset:
            params['offset'] = offset
        resp = requests.get(url, headers=headers, params=params).json()

        if 'records' not in resp:
            print(f"❌ Error fetching {table}:", resp)
            break

        all_records.extend(resp['records'])
        offset = resp.get('offset')
        if not offset:
            break

    print(f"✅ Fetched {len(all_records)} records from '{table}'")
    return all_records

# Step 1: Fetch main Feedback Form records
main_records = fetch_all_records(MAIN_TABLE, view=VIEW_NAME)
print(f"🔍 Retrieved {len(main_records)} records from {MAIN_TABLE}")

# Step 2: Stamp with timestamp so CSV always updates
timestamp = datetime.utcnow().isoformat()
for record in main_records:
    record['fields']['Last Updated'] = timestamp

# Step 3: Export wide version (all fields as-is)
wide_file = 'Feedback_Form_Data.csv'
with open(wide_file, 'w', newline='', encoding='utf-8') as csvfile:
    fieldnames = set()
    for rec in main_records:
        fieldnames.update(rec['fields'].keys())
    fieldnames = list(fieldnames)

    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for rec in main_records:
        writer.writerow(rec['fields'])

print(f"✅ Export complete (wide): {wide_file}")

# Step 4: Create long (pivoted) version for stacked bar chart
# Load wide CSV into pandas
df = pd.read_csv(wide_file)

# Define which columns to pivot (your 5 survey Qs)
pivot_cols = [
    "Knowledge Gain",
    "Application Intent",
    "Relevance to Work",
    "Challenges Addressed",
    "Sharing Intent"
]

# Melt into long format
df_long = df.melt(
    id_vars=[col for col in df.columns if col not in pivot_cols],
    value_vars=pivot_cols,
    var_name="Question",
    value_name="Response"
)

# Export long version
long_file = 'Feedback_Form_Data_Long.csv'
df_long.to_csv(long_file, index=False, encoding='utf-8')
print(f"✅ Export complete (long): {long_file}")
