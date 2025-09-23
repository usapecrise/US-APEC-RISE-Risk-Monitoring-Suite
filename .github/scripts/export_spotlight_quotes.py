import os
import pandas as pd
from pyairtable import Api

# === CONFIG ===
AIRTABLE_TOKEN = os.environ['AIRTABLE_TOKEN']  # GitHub Secret
BASE_ID = "app0Ljjhrp3lTTpTO"
TABLE_ID = "tblnUz7kmYvxk9m9a"  # use table ID instead of table name
OUTPUT_FILE = "spotlight_quotes.csv"

# === 1. Connect to Airtable ===
api = Api(AIRTABLE_TOKEN)
table = api.table(BASE_ID, TABLE_ID)

# === 2. Fetch all records
records = table.all()  # you can add view="Grid view" if needed

# === 3. Flatten into a DataFrame
rows = []
for r in records:
    f = r["fields"]
    rows.append({
        "Quote ID": r["id"],  # Airtable record ID (unique key)
        "Quote": f.get("Quote Text", ""),
        "Organization": f.get("Organization", ""),
        "Economy": f.get("Economy", ""),
        "Workshop Title": f.get("Workshop Title", "")
    })

df = pd.DataFrame(rows)

# === 4. Export to CSV
df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")

print(f"✅ Exported {len(df)} spotlight quotes to {OUTPUT_FILE}")
