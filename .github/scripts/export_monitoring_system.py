import os
import requests
import pandas as pd
from tableauhyperapi import HyperProcess, Telemetry, Connection, TableDefinition, SqlType, TableName, Inserter, CreateMode

# Airtable Config
AIRTABLE_TOKEN = os.environ["AIRTABLE_TOKEN"]
BASE_ID = "app0Ljjhrp3lTTpTO"   # Base ID from your Airtable link
TABLE_ID = "tblUWje5Fyvh5sNqu"  # Table ID from your Airtable link
VIEW_ID = None                  # Optional: e.g. "viwbGHbT05kxOWdec"

def fetch_airtable_table():
    """Fetch Monitoring System table from Airtable."""
    url = f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_ID}"
    headers = {"Authorization": f"Bearer {AIRTABLE_TOKEN}"}
    records, offset = [], None

    while True:
        params = {"view": VIEW_ID} if VIEW_ID else {}
        if offset:
            params["offset"] = offset

        resp = requests.get(url, headers=headers, params=params)
        if resp.status_code != 200:
            raise RuntimeError(f"Airtable API error {resp.status_code}: {resp.text}")

        data = resp.json()
        for r in data.get("records", []):
            f = r.get("fields", {})
            records.append({
                "Indicator": f.get("Indicator", ""),
                "Data Source": f.get("Data Source", ""),
                "Disaggregates": f.get("Disaggregates", ""),
                "Data Flow": f.get("Data Flow", "")
            })

        offset = data.get("offset")
        if not offset:
            break

    return pd.DataFrame(records)


# --------------------------
# Step 1: Fetch Airtable → CSV
# --------------------------
df = fetch_airtable_table()
df.to_csv("Monitoring_System.csv", index=False)
print("✅ Exported Monitoring_System.csv")


# --------------------------
# Step 2: Convert CSV → Hyper
# --------------------------
with HyperProcess(Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
    with Connection(endpoint=hyper.endpoint,
                    database="Monitoring_System.hyper",
                    create_mode=CreateMode.CREATE_AND_REPLACE) as connection:

        table_def = TableDefinition(TableName("public", "Monitoring_System"))
        table_def.add_column("Indicator", SqlType.text())
        table_def.add_column("DataSource", SqlType.text())
        table_def.add_column("Disaggregates", SqlType.text())
        table_def.add_column("DataFlow", SqlType.text())

        connection.catalog.create_table(table_def)

        with Inserter(connection, table_def) as inserter:
            inserter.add_rows(rows=df.values.tolist())
            inserter.execute()

print("✅ Created Monitoring_System.hyper")

