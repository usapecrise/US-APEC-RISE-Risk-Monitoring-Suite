import os
import requests
import glob
import csv
from tableauhyperapi import HyperProcess, Connection, Telemetry, TableDefinition, SqlType, Inserter, TableName, CreateMode

# 🌐 Tableau environment variables (set in GitHub secrets)
TOKEN_NAME = os.environ['TABLEAU_TOKEN_NAME']
TOKEN_SECRET = os.environ['TABLEAU_TOKEN_SECRET']
SITE_ID = os.environ['TABLEAU_SITE_ID']
PROJECT_ID = os.environ['TABLEAU_PROJECT_ID']

BASE_URL = "https://api.tableau.com/api/3.21"

print("🚦 Starting Tableau Hyper upload script")
print(f"📁 Current directory: {os.getcwd()}")
print(f"🔍 Looking for CSVs in: {os.listdir('.')}")

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
        "site": {"contentUrl": ""}
    }
}
auth_response = requests.post(f"{BASE_URL}/auth/signin", json=auth_payload, headers=auth_headers)
if auth_response.status_code != 200:
    raise Exception(f"❌ Tableau Auth Failed: {auth_response.status_code} - {auth_response.text}")

auth_data = auth_response.json()['credentials']
auth_token = auth_data['token']
site_id = auth_data['site']['id']
headers = {
    "X-Tableau-Auth": auth_token
}

# ✅ Convert each CSV and upload
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

                with Inserter(connection, table_def) as inserter:
                    with open(csv_file, 'r', encoding='utf-8-sig') as f:
                        reader = csv.reader(f)
                        next(reader)
                        for row in reader:
                            inserter.add_row(row)
                    inserter.execute()
                print(f"✅ Finished inserting rows from {csv_file}")

        # Upload step: initiate file upload
        print(f"🚀 Initiating upload of {hyper_file} to Tableau...")
        upload_session_url = f"{BASE_URL}/sites/{site_id}/fileUploads"
        upload_session_resp = requests.post(upload_session_url, headers=headers)
        if upload_session_resp.status_code != 201:
            raise Exception(f"❌ Upload session failed: {upload_session_resp.text}")
        upload_session_id = upload_session_resp.json()['fileUpload']['uploadSessionId']

        # Upload the file
        with open(hyper_file, 'rb') as f:
            put_headers = {
                'Content-Type': 'application/octet-stream',
                'X-Tableau-Auth': auth_token
            }
            put_url = f"{BASE_URL}/sites/{site_id}/fileUploads/{upload_session_id}"
            put_resp = requests.put(put_url, data=f, headers=put_headers)
            if put_resp.status_code != 200:
                raise Exception(f"❌ PUT failed: {put_resp.status_code} - {put_resp.text}")

        # Publish the datasource
        publish_url = f"{BASE_URL}/sites/{site_id}/datasources?uploadSessionId={upload_session_id}&datasourceType=hyper&overwrite=true"
        xml_payload = f"""
        <tsRequest>
            <datasource name="{base_name}">
                <project id="{PROJECT_ID}" />
            </datasource>
        </tsRequest>
        """
        pub_headers = {
            'Content-Type': 'application/xml',
            'X-Tableau-Auth': auth_token
        }
        pub_resp = requests.post(publish_url, data=xml_payload.encode('utf-8'), headers=pub_headers)
        if pub_resp.status_code == 201:
            print(f"✅ Successfully uploaded {base_name}.hyper to Tableau")
        else:
            print(f"❌ Failed to publish {base_name}.hyper")
            print(f"Status: {pub_resp.status_code}, Message: {pub_resp.text}")

        os.remove(hyper_file)

    except Exception as e:
        print(f"🔥 Error processing {csv_file}: {e}")

# 🚪 Sign out
requests.post(f"{BASE_URL}/auth/signout", headers=headers)
print("\n👋 Signed out of Tableau")

