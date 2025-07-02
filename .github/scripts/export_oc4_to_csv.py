import requests
import pandas as pd

# === Airtable Config ===
AIRTABLE_API_KEY = "your_airtable_api_key"
BASE_ID = "your_base_id"
TABLE_NAME = "OC4"

# === Airtable API URL ===
url = f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_NAME}"
headers = {
    "Authorization": f"Bearer {AIRTABLE_API_KEY}"
}

# === Fetch records ===
all_records = []
offset = None

while True:
    params = {}
    if offset:
        params["offset"] = offset
    response = requests.get(url, headers=headers, params=params)
    data = response.json()

    for record in data["records"]:
        flat = record["fields"]
        flat["Record ID"] = record["id"]
        all_records.append(flat)

    offset = data.get("offset")
    if not offset:
        break

# === Convert to DataFrame and export ===
df = pd.DataFrame(all_records)
df["Positive?"] = df["Response"].apply(lambda x: 1 if x == "Yes" else 0)
df.to_csv("OC4.csv", index=False)
