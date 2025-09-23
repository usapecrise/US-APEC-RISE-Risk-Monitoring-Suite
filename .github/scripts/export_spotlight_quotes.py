import os
import pandas as pd
from pyairtable import Api

# === CONFIG ===
AIRTABLE_TOKEN = os.environ['AIRTABLE_TOKEN']
BASE_ID = "app0Ljjhrp3lTTpTO"
MAIN_TABLE =  "Spotlight Quotes"
VIEW_NAME = 'Grid view'
OUTPUT_FILE = "spotlight_quotes.csv"

# === 1. Connect to Airtable ===
api = Api(AIRTABLE_API_KEY)
table = api.table(BASE_ID, TABLE_NAME)

# === 2. Fetch all records ===
records = table.all()

# === 3. Flatten into a DataFrame ===
rows = []
for r in records:
    f = r["fields"]
    rows.append({
        "Quote ID": r["id"],   # Airtable record ID (unique)
        "Quote": f.get("Quote Text", ""),
        "Organization": f.get("Organization", ""),
        "Economy": f.get("Economy", ""),
        "Workshop Title": f.get("Workshop Title", "")
    })

df = pd.DataFrame(rows)

# === 4. Export to CSV ===
df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")

print(f"✅ Exported {len(df)} spotlight quotes to {OUTPUT_FILE}")
