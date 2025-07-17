import os
import glob
import requests
from tableauhyperapi import (
    HyperProcess, Telemetry, Connection, CreateMode,
    SqlType, TableDefinition, Inserter, TableName
)

# Load environment variables
TOKEN_NAME = os.environ['TABLEAU_TOKEN_NAME']
TOKEN_SECRET = os.environ['TABLEAU_TOKEN_SECRET']
SITE_ID = os.environ['TABLEAU_SITE_ID']
PROJECT_ID = os.environ['TABLEAU_PROJECT_ID']
API_VERSION = "3.21"
BASE_URL = f"https://prod-useast-a.online.tableau.com/api/{API_VERSION}"

# Step 1: Sign in to Tableau
signin_payload = {
    "credentials": {
        "personalAccessTokenName": TOKEN_NAME,
        "personalAccessTokenSecret": TOKEN_SECRET,
        "site": {"contentUrl": SITE_ID}
    }
}
auth_response = requests.post(f"{BASE_URL}/auth/signin", json=signin_payload)
auth_response.raise_for_status()
auth_data = auth_response.json()
auth_token = auth_data["credentials"]["token"]
site_id = auth_data["credentials"]["site"]["id"]
headers = {"X-Tableau-Auth": auth_token}

# Step 2: Convert all CSV files in root to HYPER and upload
csv_files = glob.glob("*.csv")
print(f"🔍 Found {len(csv_files)} CSV files: {csv_files}")

for csv_file in csv_files:
    base_name = os.path.splitext(csv_file)[0]
    hyper_file = f"{base_name}.hyper"

    # Step 2a: Convert CSV → HYPER
    print(f"⚙️ Converting {csv_file} to {hyper_file}...")
    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(endpoint=hyper.endpoint, database=hyper_file, create_mode=CreateMode.CREATE_AND_REPLACE) as connection:
            table_name = TableName("Extract", "Extract")
            with open(csv_file, 'r', encoding='utf-8-sig') as f:
                header_line = f.readline().strip()
            columns = header_line.split(",")
            table_def = TableDefinition(table_name=table_name)
            for col in columns:
                table_def.add_column(col.strip(), SqlType.text())

            connection.catalog.create_table(table_def)

            with Inserter(connection, table_def) as inserter:
                inserter.add_rows_from_csv(csv_file)
                inserter.execute()

    # Step 2b: Upload .hyper to Tableau
    print(f"🚀 Uploading {hyper_file} to Tableau as datasource '{base_name}'...")
    with open(hyper_file, 'rb') as file_data:
        file_payload = {
            'request_payload': (
                None,
                f"""
                <tsRequest>
                  <datasource name="{base_name}">
                    <project id="{PROJECT_ID}" />
                  </datasource>
                </tsRequest>
                """,
                'text/xml'
            ),
            'tableau_datasource': (hyper_file, file_data, 'application/octet-stream')
        }
        upload_url = f"{BASE_URL}/sites/{site_id}/datasources?overwrite=true"
        response = requests.post(upload_url, files=file_payload, headers=headers)
        if response.status_code == 201:
            print(f"✅ Successfully published {base_name}.hyper")
        else:
            print(f"❌ Failed to publish {base_name}.hyper\n{response.text}")

# Step 3: Sign out
requests.post(f"{BASE_URL}/auth/signout", headers=headers)
print("🔒 Signed out of Tableau session.")
