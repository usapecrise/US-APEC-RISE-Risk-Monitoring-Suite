import os
import requests
import pandas as pd
from tableauhyperapi import HyperProcess, Telemetry, Connection, TableDefinition, SqlType, TableName, Inserter, CreateMode
import tableauserverclient as TSC

# Airtable config
AIRTABLE_TOKEN = os.environ["AIRTABLE_TOKEN"]
BASE_ID = "appXXXXXXXXXXXXXX"   # replace with your Airtable Base ID
TABLE_ID = "tblXXXXXXXXXXXXXX"  # replace with your Airtable Table ID

def fetch_airtable_table():
    url = f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_ID}"
    headers = {"Authorization": f"Bearer {AIRTABLE_TOKEN}"}
    records, offset = [], None

    while True:
        params = {}
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
                "DataSource": f.get("Data Source", ""),
                "Disaggregates": f.get("Disaggregates", ""),
                "DataFlow": f.get("Data Flow", "")
            })

        offset = data.get("offset")
        if not offset:
            break

    return pd.DataFrame(records)

# Step 1: Fetch from Airtable
df = fetch_airtable_table()
df.to_csv("Monitoring_System.csv", index=False)
print("✅ Exported Monitoring_System.csv")

# Step 2: Convert to .hyper
with HyperProcess(Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
    with Connection(endpoint=hyper.endpoint,
                    database="Monitoring_System.hyper",
                    create_mode=CreateMode.CREATE_AND_REPLACE) as connection:

        table_def = TableDefinition(
            TableName("public", "Monitoring_System"),
            [
                ("Indicator", SqlType.text()),
                ("DataSource", SqlType.text()),
                ("Disaggregates", SqlType.text()),
                ("DataFlow", SqlType.text())
            ]
        )
        connection.catalog.create_table(table_def)

        with Inserter(connection, table_def) as inserter:
            inserter.add_rows(rows=df.values.tolist())
            inserter.execute()

print("✅ Created Monitoring_System.hyper")

# Step 3: Publish to Tableau
TABLEAU_SERVER = "https://your-tableau-server.com"   # Tableau Cloud/Server URL
TABLEAU_PROJECT = "US APEC-RISE"                     # Project name in Tableau
TABLEAU_DATASOURCE = "Monitoring System Data"        # Name for datasource
USERNAME = os.environ["TABLEAU_USER"]
PASSWORD = os.environ["TABLEAU_PASS"]

tableau_auth = TSC.TableauAuth(USERNAME, PASSWORD, site="")
server = TSC.Server(TABLEAU_SERVER, use_server_version=True)

with server.auth.sign_in(tableau_auth):
    datasource = TSC.DatasourceItem(project_id=None, name=TABLEAU_DATASOURCE)
    server.datasources.publish(datasource, "Monitoring_System.hyper", mode=TSC.Server.PublishMode.Overwrite)

print("✅ Published Monitoring System Data to Tableau")
