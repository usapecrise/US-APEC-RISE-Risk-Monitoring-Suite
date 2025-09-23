import os
import pandas as pd
from pyairtable import Api

# === CONFIG ===
AIRTABLE_TOKEN = os.environ['AIRTABLE_TOKEN']
BASE_ID = "app0Ljjhrp3lTTpTO"
TABLE_ID = "tblnUz7kmYvxk9m9a"  # Spotlight Quotes
OUTPUT_FILE = "spotlight_quotes.csv"

# Linked table names
LINKED_TABLES = {
    "Economy": "Economy Reference List",
}

DISPLAY_FIELDS = {
    "Economy": "Economy",
}

# === 1. Connect to Airtable ===
api = Api(AIRTABLE_TOKEN)
table = api.table(BASE_ID, TABLE_ID)

# === 2. Fetch all records ===
records = table.all(view="Grid view")

# === 3. Preload lookup values for linked tables ===
lookup_maps = {}
for field, linked_table in LINKED_TABLES.items():
    linked_tbl = api.table(BASE_ID, linked_table)
    linked_records = linked_tbl.all()
    lookup_maps[field] = {
        rec["id"]: rec["fields"].get(DISPLAY_FIELDS[field], "")
        for rec in linked_records
    }

# === 4. Flatten Spotlight Quotes ===
rows = []
for r in records:
    f = r["fields"]

    resolved = {}
    for field, mapping in lookup_maps.items():
        linked_ids = f.get(field, [])
        if isinstance(linked_ids, list):
            resolved[field] = ", ".join(mapping.get(lid, lid) for lid in linked_ids)
        else:
            resolved[field] = mapping.get(linked_ids, linked_ids)

    rows.append({
        "Quote Text": f.get("Quote Text", ""),
        "Organization": f.get("Organization", ""),
        "Economy": resolved.get("Economy", ""),
        "Workshop Title": f.get("Workshop Title", ""),
    })

df = pd.DataFrame(rows)

# === 5. Add numeric Quote ID (1,2,3...)
df.insert(0, "Quote ID", range(1, len(df) + 1))

# === 6. Export ===
df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")
print(f"✅ Exported {len(df)} spotlight quotes to {OUTPUT_FILE}")
