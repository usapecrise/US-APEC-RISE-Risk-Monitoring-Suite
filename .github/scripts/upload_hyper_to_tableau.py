import os
import csv
import requests
import uuid
from tableauhyperapi import HyperProcess, Connection, TableDefinition, SqlType, Telemetry, Inserter, CreateMode

# Tableau credentials from environment variables
TABLEAU_PAT_NAME = os.environ["TABLEAU_PAT_NAME"]
TABLEAU_PAT_SECRET = os.environ["TABLEAU_PAT_SECRET"]
TABLEAU_SITE_ID = os.environ["TABLEAU_SITE_ID"]
TABLEAU_PROJECT_ID = os.environ["TABLEAU_PROJECT_ID"]
BASE_URL = "https://prod-useast-a.online.tableau.com/api/3.21"

def create_multipart_payload(hyper_path, datasource_name, project_id):
    boundary = f"----WebKitFormBoundary{uuid.uuid4().hex}"
    with open(hyper_path, "rb") as f:
        hyper_data = f.read()

    xml_part = f"""--{boundary}
Content-Disposition: name="request_payload"
Content-Type: text/xml

<tsRequest>
  <datasource name="{datasource_name}">
    <project id="{project_id}" />
  </datasource>
</tsRequest>
"""

    hyper_part = f"""--{boundary}
Content-Disposition: name="tableau_datasource"; filename="{datasource_name}.hyper"
Content-Type: application/octet-stream

""".encode("utf-8") + hyper_data + f"\n--{boundary}--".encode("utf-8")

    return boundary, xml_part.encode("utf-8") + hyper_part

print("🚦 Starting Tableau Hyper upload script")
print(f"📁 Current directory: {os.getcwd()}")

csv_files = [f for f in os.listdir(".") if f.endswith(".csv")]
print(f"🗂️ Matched CSV files: {csv_files}")

# Authenticate
print("🔑 Authenticating with Tableau...")
auth_resp = requests.post(
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
auth_resp.raise_for_status()
auth_data = auth_resp.json()
auth_token = auth_data["credentials"]["token"]
site_id = auth_data["credentials"]["site"]["id"]
print("✅ Tableau auth successful\n")

# Process each CSV
for csv_file in csv_files:
    hyper_name = csv_file.replace(".csv", ".hyper")
    print(f"⚙️ Converting {csv_file} to {hyper_name}...")

    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(endpoint=hyper.endpoint, database=hyper_name, create_mode=CreateMode.CREATE_AND_REPLACE) as connection:
            with open(csv_file, "r", encoding="utf-8-sig") as f:
                headers = f.readline().strip().split(",")

            table_def = TableDefinition(table_name="Extract")
            for col in headers:
                table_def.add_column(col, SqlType.text())
            connection.catalog.create_table(table_def)

            with Inserter(connection, table_def) as inserter:
                with open(csv_file, "r", encoding="utf-8-sig") as f:
                    reader = csv.reader(f)
                    next(reader)
                    inserter.add_rows(reader)
                inserter.execute()

    print(f"📦 Created {hyper_name} ({os.path.getsize(hyper_name)} bytes)")

    print(f"📤 Publishing {hyper_name} to Tableau...")
    boundary, payload = create_multipart_payload(hyper_name, csv_file.replace(".csv", ""), TABLEAU_PROJECT_ID)

    publish_url = f"{BASE_URL}/sites/{site_id}/datasources?datasourceType=hyper"
    publish_resp = requests.post(
        publish_url,
        headers={
            "X-Tableau-Auth": auth_token,
            "Content-Type": f"multipart/mixed; boundary={boundary}"
        },
        data=payload
    )

    if publish_resp.status_code == 201:
        print(f"✅ Published {csv_file} as a datasource.\n")
    else:
        print(f"🔥 Failed to publish {csv_file}: {publish_resp.status_code}")
        print(f"🔍 Response: {publish_resp.text}\n")

# Sign out
requests.post(f"{BASE_URL}/auth/signout", headers={"X-Tableau-Auth": auth_token})
print("🚪 Signed out of Tableau")
