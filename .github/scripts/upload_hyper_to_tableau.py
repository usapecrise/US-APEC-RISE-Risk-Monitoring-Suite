import os
import requests
import glob
import csv
import time
from tableauhyperapi import HyperProcess, Connection, Telemetry, TableDefinition, SqlType, Inserter, TableName, CreateMode

# 🔐 Tableau credentials from GitHub Secrets
TOKEN_NAME = os.environ['TABLEAU_TOKEN_NAME']
TOKEN_SECRET = os.environ['TABLEAU_TOKEN_SECRET']
SITE_ID_ENV = os.environ['TABLEAU_SITE_ID']
PROJECT_ID = os.environ['TABLEAU_PROJECT_ID']

# 📡 Tableau base URL
BASE_URL = "https://prod-useast-a.online.tableau.com/api/3.21"

print("🚦 Starting Tableau Hyper upload script")
print(f"📁 Current directory: {os.getcwd()}")
print(f"🔍 Looking for CSVs in: {os.listdir('.')}")

dataset_files = glob.glob("*.csv")
print(f"🗂️ Matched CSV files: {dataset_files}")
if not dataset_files:
    raise FileNotFoundError("🚫 No CSV files found!")

# 🔐 Sign in to Tableau
print("\n🔑 Authenticating with Tableau...")
auth_response = requests.post(
    f"{BASE_URL}/auth/signin",
    json={
        "credentials": {
            "personalAccessTokenName": TOKEN_NAME,
            "personalAccessTokenSecret": TOKEN_SECRET,
            "site": {"contentUrl": SITE_ID_ENV}
        }
    },
    headers={"Content-Type": "application/json", "Accept": "application/json"}
)

if auth_response.status_code != 200:
    print("❌ Authentication failed")
    print(auth_response.text)
    exit(1)

auth_data = auth_response.json()["credentials"]
auth_token = auth_data["token"]
site_id = auth_data["site"]["id"]
headers = {"X-Tableau-Auth": auth_token}

# ✅ Loop through CSVs
for csv_file in dataset_files:
    try:
        base_name = os.path.splitext(csv_file)[0]
        hyper_file = f"{base_name}.hyper"

        print(f"\n⚙️ Converting {csv_file} to {hyper_file}")
        with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
            with Connection(endpoint=hyper.endpoint, database=hyper_file, create_mode=CreateMode.CREATE_AND_REPLACE) as connection:
                table_name = TableName("Extract", "Extract")
                connection.catalog.create_schema("Extract")

                with open(csv_file, "r", encoding="utf-8-sig") as f:
                    reader = csv.reader(f)
                    headers_row = next(reader)
                    columns = [col.strip() for col in headers_row]

                table_def = TableDefinition(table_name)
                for col in columns:
                    table_def.add_column(col, SqlType.text())
                connection.catalog.create_table(table_def)

                with Inserter(connection, table_def) as inserter:
                    with open(csv_file, "r", encoding="utf-8-sig") as f:
                        reader = csv.reader(f)
                        next(reader)  # Skip headers
                        for row in reader:
                            inserter.add_row(row)
                    inserter.execute()

        # Step 1: Upload file to Tableau
        print(f"🚀 Uploading {hyper_file} to Tableau staging...")
        with open(hyper_file, 'rb') as f:
            upload_response = requests.post(
                f"{BASE_URL}/sites/{site_id}/fileUploads",
                headers=headers,
                files={"file": f}
            )

        if upload_response.status_code != 200:
            raise Exception(f"File upload failed: {upload_response.text}")

        upload_session_id = upload_response.json()['fileUpload']['uploadSessionId']

        # Step 2: Publish .hyper from staging to Tableau
        print(f"📦 Publishing {base_name}.hyper to Tableau project...")
        xml_payload = f"""
        <tsRequest>
            <datasource name="{base_name}" >
                <project id="{PROJECT_ID}" />
            </datasource>
        </tsRequest>
        """.strip()

        multipart_payload = {
            "request_payload": ("", xml_payload, "text/xml"),
            "tableau_datasource": (hyper_file, open(hyper_file, "rb"), "application/octet-stream")
        }

        publish_response = requests.post(
            f"{BASE_URL}/sites/{site_id}/datasources?uploadSessionId={upload_session_id}&datasourceType=hyper&overwrite=true",
            headers=headers,
            files=multipart_payload
        )

        if publish_response.status_code == 201:
            print(f"✅ Successfully published {base_name}.hyper to Tableau")
        else:
            print(f"❌ Failed to publish {base_name}.hyper")
            print(f"Status: {publish_response.status_code}")
            print(publish_response.text)

        os.remove(hyper_file)

    except Exception as e:
        print(f"🔥 Error with {csv_file}: {e}")

# 🔒 Sign out
requests.post(f"{BASE_URL}/auth/signout", headers=headers)
print("🚪 Signed out of Tableau")
