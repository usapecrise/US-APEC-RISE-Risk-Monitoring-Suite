import os
import requests
import glob
import csv
import time
from tableauhyperapi import HyperProcess, Connection, Telemetry, TableDefinition, SqlType, Inserter, TableName, CreateMode

# 🌐 Tableau environment variables (set in GitHub secrets)
TOKEN_NAME = os.environ['TABLEAU_TOKEN_NAME']
TOKEN_SECRET = os.environ['TABLEAU_TOKEN_SECRET']
SITE_ID = os.environ['TABLEAU_SITE_ID']  # Site content ID like "thecadmusgrouponline"
PROJECT_ID = os.environ['TABLEAU_PROJECT_ID']

# 📡 Tableau base URL
BASE_URL = f"https://prod-useast-a.online.tableau.com/api/3.21"

print("🚦 Starting Tableau Hyper upload script")
print(f"📁 Current directory: {os.getcwd()}")
print(f"🔍 Looking for CSVs in: {os.listdir('.')}")

# 🔍 Find all CSV files in repo root
dataset_files = glob.glob("*.csv")
print(f"🗂️ Matched CSV files: {dataset_files}")
if not dataset_files:
    raise FileNotFoundError("🚫 No CSV files found! Check directory or glob pattern.")

# 🔐 Authenticate with Tableau with retries
print("\n🔑 Authenticating with Tableau...")
auth_headers = {"Content-Type": "application/json", "Accept": "application/json"}
auth_payload = {
    "credentials": {
        "personalAccessTokenName": TOKEN_NAME,
        "personalAccessTokenSecret": TOKEN_SECRET,
        "site": {"contentUrl": SITE_ID}
    }
}

MAX_RETRIES = 3
RETRY_DELAY = 10
for attempt in range(MAX_RETRIES):
    try:
        auth_response = requests.post(f"{BASE_URL}/auth/signin", json=auth_payload, headers=auth_headers, timeout=30)
        if auth_response.status_code == 200:
            print("✅ Tableau auth successful")
            break
        else:
            print(f"❌ Tableau auth failed: {auth_response.status_code}")
            print(auth_response.text)
            raise Exception("Failed auth status")
    except requests.exceptions.RequestException as e:
        print(f"🔁 Retry {attempt + 1}/{MAX_RETRIES} due to error: {e}")
        time.sleep(RETRY_DELAY)
else:
    raise Exception("❌ Failed to authenticate after multiple retries")

auth_data = auth_response.json()['credentials']
auth_token = auth_data['token']
site_id = auth_data['site']['id']
headers = {"X-Tableau-Auth": auth_token}

# ✅ Loop through and upload each CSV
for csv_file in dataset_files:
    try:
        print(f"\n⚙️ Converting {csv_file} to .hyper...")
        base_name = os.path.splitext(csv_file)[0]
        hyper_file = f"{base_name}.hyper"

        # Convert CSV to Hyper
        with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
            with Connection(endpoint=hyper.endpoint, database=hyper_file, create_mode=CreateMode.CREATE_AND_REPLACE) as connection:
                print(f"📂 Connected to Hyper engine for {csv_file}")
                table_name = TableName("Extract", "Extract")
                connection.catalog.create_schema("Extract")

                with open(csv_file, 'r', encoding='utf-8-sig') as f:
                    reader = csv.reader(f)
                    headers_row = next(reader)
                    columns = [col.strip() for col in headers_row]

                table_def = TableDefinition(table_name=table_name)
                for col in columns:
                    table_def.add_column(col, SqlType.text())
                connection.catalog.create_table(table_def)
                print(f"🧱 Table created: {columns}")

                with Inserter(connection, table_def) as inserter:
                    with open(csv_file, 'r', encoding='utf-8-sig') as f:
                        reader = csv.reader(f)
                        next(reader)
                        for row in reader:
                            inserter.add_row(row)
                    inserter.execute()
                print(f"✅ Finished inserting rows from {csv_file}")

        # ⬆️ Upload to Tableau using fileUpload and publish
        file_size = os.path.getsize(hyper_file)
        print(f"📦 Size of {hyper_file}: {file_size} bytes")
        print(f"🚀 Uploading {hyper_file} to Tableau staging...")

        # Step 1: Initiate upload session
        session_resp = requests.post(f"{BASE_URL}/sites/{site_id}/fileUploads", headers=headers)
        upload_id = session_resp.json()["fileUpload"]["uploadSessionId"]

        # Step 2: Upload hyper binary to session
        with open(hyper_file, "rb") as f:
            upload_resp = requests.put(
                f"{BASE_URL}/sites/{site_id}/fileUploads/{upload_id}",
                data=f,
                headers={
                    "X-Tableau-Auth": auth_token,
                    "Content-Type": "application/octet-stream"
                }
            )
        if upload_resp.status_code != 200:
            raise Exception(f"🔥 Upload failed: {upload_resp.text}")
        print(f"📤 Uploaded {hyper_file} (session ID: {upload_id})")

        # Step 3: Publish as datasource
        publish_url = f"{BASE_URL}/sites/{site_id}/datasources?uploadSessionId={upload_id}&datasourceType=hyper&overwrite=true"
        xml_payload = f"""
        <tsRequest>
            <datasource name="{base_name}">
                <project id="{PROJECT_ID}" />
            </datasource>
        </tsRequest>
        """
        publish_resp = requests.post(
            publish_url,
            headers={**headers, "Content-Type": "application/xml"},
            data=xml_payload.encode("utf-8")
        )

        if publish_resp.status_code == 201:
            print(f"✅ Published {base_name}.hyper to Tableau")
        else:
            print(f"❌ Failed to publish {base_name}.hyper")
            print(f"Status: {publish_resp.status_code}, Message: {publish_resp.text}")

        os.remove(hyper_file)

    except Exception as e:
        print(f"🔥 Error with {csv_file}: {e}")

# 🚪 Sign out
requests.post(f"{BASE_URL}/auth/signout", headers=headers)
print("\n🚪 Signed out of Tableau")
