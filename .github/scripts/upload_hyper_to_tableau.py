import os
import csv
import requests
import xml.etree.ElementTree as ET
from tableauhyperapi import HyperProcess, Connection, TableDefinition, SqlType, Telemetry, Inserter, CreateMode

# Environment variables
TABLEAU_PAT_NAME = os.environ["TABLEAU_PAT_NAME"]
TABLEAU_PAT_SECRET = os.environ["TABLEAU_PAT_SECRET"]
TABLEAU_SITE_ID = os.environ["TABLEAU_SITE_ID"]
TABLEAU_USER_ID = os.environ["TABLEAU_USER_ID"]
TABLEAU_PROJECT_ID = os.environ["TABLEAU_PROJECT_ID"]
BASE_URL = "https://prod-useast-a.online.tableau.com/api/3.21"

print("🚦 Starting Tableau Hyper upload script")
print(f"📁 Current directory: {os.getcwd()}")

# List all CSVs
csv_files = [f for f in os.listdir(".") if f.endswith(".csv")]
print(f"🗂️ Matched CSV files: {csv_files}")

# Authenticate
print("🔑 Authenticating with Tableau...")
auth_response = requests.post(
    f"{BASE_URL}/auth/signin",
    headers={"Accept": "application/json"},
    json={
        "credentials": {
            "personalAccessTokenName": TABLEAU_PAT_NAME,
            "personalAccessTokenSecret": TABLEAU_PAT_SECRET,
            "site": {"contentUrl": TABLEAU_SITE_ID}
        }
    }
)

print(f"🔍 Auth response status: {auth_response.status_code}")
print(f"🔍 Auth response body:\n{auth_response.text}")

try:
    auth_response.raise_for_status()
    auth_data = auth_response.json()
except Exception as e:
    print(f"❌ Auth error: {e}")
    exit(1)

auth_token = auth_data["credentials"]["token"]
site_id = auth_data["credentials"]["site"]["id"]
user_id = auth_data["credentials"]["user"]["id"]
print("✅ Tableau auth successful\n")

# Process each CSV
for csv_file in csv_files:
    hyper_name = csv_file.replace(".csv", ".hyper")
    print(f"⚙️ Converting {csv_file} to .hyper...")

    # Convert CSV to .hyper
    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(endpoint=hyper.endpoint, database=hyper_name, create_mode=CreateMode.CREATE_AND_REPLACE) as connection:
            with open(csv_file, "r", encoding="utf-8-sig") as f:
                header = f.readline().strip().split(",")
            table_def = TableDefinition(table_name="Extract")
            for col in header:
                table_def.add_column(col, SqlType.text())
            connection.catalog.create_table(table_def)

            with Inserter(connection, table_def) as inserter:
                with open(csv_file, "r", encoding="utf-8-sig") as f:
                    reader = csv.reader(f)
                    next(reader)  # Skip header
                    inserter.add_rows(reader)
                inserter.execute()

    print(f"📦 Size of {hyper_name}: {os.path.getsize(hyper_name)} bytes")

    print(f"🚀 Uploading {hyper_name} to Tableau staging...")
    upload_req = requests.post(
        f"{BASE_URL}/sites/{site_id}/fileUploads",
        headers={"X-Tableau-Auth": auth_token}
    )

    if upload_req.status_code not in (200, 201):
        print(f"🔥 Upload session failed: {upload_req.status_code}")
        print(f"🔍 Response: {upload_req.text}")
        continue

    try:
        root = ET.fromstring(upload_req.text)
        ns = {"t": "http://tableau.com/api"}
        upload_id = root.find(".//t:fileUpload", ns).attrib["uploadSessionId"]
    except Exception as e:
        print("❌ Failed to parse upload response as XML.")
        print(f"🔍 Error: {e}")
        continue

    with open(hyper_name, 'rb') as f:
        files = {
            'request_payload': (None, '', 'text/plain'),
            'tableau_file': (hyper_name, f, 'application/octet-stream')
        }
        upload_resp = requests.post(
            f"{BASE_URL}/sites/{site_id}/fileUploads/{upload_id}",
            headers={"X-Tableau-Auth": auth_token},
            files=files
        )

    if upload_resp.status_code != 200:
        print(f"🔥 Upload failed for {hyper_name}: Status {upload_resp.status_code}")
        print(f"🔍 Raw response: {upload_resp.text if upload_resp.text else '[No response body]'}")
        continue

    print(f"✅ Uploaded {hyper_name} to staging")

    print(f"📡 Publishing {hyper_name} as data source...")
    publish_url = f"{BASE_URL}/sites/{site_id}/datasources?uploadSessionId={upload_id}&datasourceType=hyper"
    xml_payload = f"""
    <tsRequest>
      <datasource name="{csv_file.replace('.csv', '')}">
        <project id="{TABLEAU_PROJECT_ID}" />
      </datasource>
    </tsRequest>
    """.strip()

    publish_resp = requests.post(
        publish_url,
        headers={
            "X-Tableau-Auth": auth_token,
            "Content-Type": "application/xml"
        },
        data=xml_payload.encode("utf-8")
    )

    if publish_resp.status_code == 201:
        print(f"✅ Published {csv_file.replace('.csv', '')} to Tableau project.\n")
    else:
        print(f"🔥 Publish failed for {csv_file}: Status {publish_resp.status_code}")
        print(f"🔍 Response: {publish_resp.text}\n")

# Sign out
requests.post(
    f"{BASE_URL}/auth/signout",
    headers={"X-Tableau-Auth": auth_token}
)
print("🚪 Signed out of Tableau")
