import os
import requests
import glob
import csv
from tableauhyperapi import HyperProcess, Connection, Telemetry, TableDefinition, SqlType, Inserter, TableName, CreateMode

# 🌐 Tableau environment variables (set in GitHub secrets)
TOKEN_NAME = os.environ['TABLEAU_TOKEN_NAME']
TOKEN_SECRET = os.environ['TABLEAU_TOKEN_SECRET']
SITE_CONTENT_URL = "thecadmusgrouponline"
PROJECT_ID = os.environ['TABLEAU_PROJECT_ID']

# 📡 Tableau REST API base
BASE_URL = "https://api.tableau.com/api/3.21"

print("🚦 Starting Tableau Hyper upload script")
print(f"📁 Current directory: {os.getcwd()}")
print(f"🔍 Looking for CSVs in: {os.listdir('.')}")

# 🔍 Find all CSV files in repo root
dataset_files = glob.glob("*.csv")
print(f"🗂️ Matched CSV files: {dataset_files}")

if not dataset_files:
    raise FileNotFoundError("🚫 No CSV files found! Check directory or glob pattern.")

# 🔐 Authenticate with Tableau
print("\n🔑 Authenticating with Tableau...")
auth_headers = {
    "Content-Type": "application/json",
    "Accept": "application/json"
}
auth_payload = {
    "credentials": {
        "personalAccessTokenName": TOKEN_NAME,
        "personalAccessTokenSecret": TOKEN_SECRET,
        "site": {"contentUrl": SITE_CONTENT_URL}
    }
}

auth_response = requests.post(f"{BASE_URL}/auth/signin", json=auth_payload, headers=auth_headers)

if auth_response.status_code != 200:
    raise Exception(f"❌ Tableau Auth Failed: {auth_response.status_code} - {auth_response.text}")

auth_data = auth_response.json()['credentials']
auth_token = auth_data['token']
site_id = auth_data['site']['id']
user_id = auth_data['user']['id']
headers = {
    "X-Tableau-Auth": auth_token
}

# ✅ Loop through and convert/upload each CSV
for csv_file in dataset_files:
    try:
        print(f"\n⚙️ Converting {csv_file} to .hyper...")

        base_name = os.path.splitext(csv_file)[0]
        hyper_file = f"{base_name}.hyper"

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

        print(f"🚀 Uploading {hyper_file} to Tableau...")

        # Upload in two steps: initiate -> append -> publish
        upload_session_url = f"{BASE_URL}/sites/{site_id}/fileUploads"
        upload_response = requests.post(upload_session_url, headers=headers)

        if upload_response.status_code != 201:
            raise Exception(f"❌ Upload session failed: {upload_response.status_code} - {upload_response.text}")

        upload_session_id = upload_response.json()["fileUpload"]["uploadSessionId"]

        with open(hyper_file, 'rb') as f:
            put_url = f"{BASE_URL}/sites/{site_id}/fileUploads/{upload_session_id}"
            put_response = requests.put(put_url, headers=headers, data=f)

        if put_response.status_code != 200:
            raise Exception(f"❌ Upload append failed: {put_response.status_code} - {put_response.text}")

        # Now publish the data source
        publish_url = f"{BASE_URL}/sites/{site_id}/datasources?uploadSessionId={upload_session_id}&datasourceType=hyper&overwrite=true"
        publish_payload = f"""
        <tsRequest>
          <datasource name="{base_name}">
            <project id="{PROJECT_ID}" />
          </datasource>
        </tsRequest>
        """

        publish_headers = headers.copy()
        publish_headers["Content-Type"] = "application/xml"

        publish_response = requests.post(publish_url, headers=publish_headers, data=publish_payload.encode("utf-8"))

        if publish_response.status_code == 201:
            print(f"✅ Uploaded {base_name}.hyper to Tableau")
        else:
            print(f"❌ Failed to publish {base_name}.hyper")
            print(f"Status: {publish_response.status_code}, Message: {publish_response.text}")

        os.remove(hyper_file)

    except Exception as e:
        print(f"🔥 Error processing {csv_file}: {e}")

# ❄️ Sign out of Tableau
requests.post(f"{BASE_URL}/auth/signout", headers=headers)
print("\n🚪 Signed out of Tableau")
