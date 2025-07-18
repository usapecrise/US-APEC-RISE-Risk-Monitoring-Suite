import os
import glob
import requests
from tableauhyperapi import HyperProcess, Connection, TableDefinition, SqlType, Telemetry, Inserter, CreateMode

# Tableau credentials from environment variables
TABLEAU_PAT_NAME = os.environ["TABLEAU_PAT_NAME"]
TABLEAU_PAT_SECRET = os.environ["TABLEAU_PAT_SECRET"]
TABLEAU_SITE_ID = os.environ["TABLEAU_SITE_ID"]
TABLEAU_USER_ID = os.environ["TABLEAU_USER_ID"]
TABLEAU_PROJECT_ID = os.environ["TABLEAU_PROJECT_ID"]
BASE_URL = "https://prod-useast-a.online.tableau.com/api/3.21"

print("\U0001F6A6 Starting Tableau Hyper upload script")
print(f"\U0001F4C1 Current directory: {os.getcwd()}")

# Locate all CSVs
csv_files = [f for f in os.listdir(".") if f.endswith(".csv")]
print(f"\U0001F5C2️ Matched CSV files: {csv_files}")

# Authenticate to Tableau
print("\U0001F511 Authenticating with Tableau...")
auth_response = requests.post(
    f"{BASE_URL}/auth/signin",
    json={
        "credentials": {
            "personalAccessTokenName": TABLEAU_PAT_NAME,
            "personalAccessTokenSecret": TABLEAU_PAT_SECRET,
            "site": {"contentUrl": TABLEAU_SITE_ID}
        }
    }
)
auth_response.raise_for_status()
auth_data = auth_response.json()
auth_token = auth_data["credentials"]["token"]
site_id = auth_data["credentials"]["site"]["id"]
user_id = auth_data["credentials"]["user"]["id"]
print("✅ Tableau auth successful\n")

# Start Hyper conversion and upload
for csv_file in csv_files:
    hyper_name = csv_file.replace(".csv", ".hyper")
    print(f"\u2699\ufe0f Converting {csv_file} to .hyper...")

    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(endpoint=hyper.endpoint, database=hyper_name, create_mode=CreateMode.CREATE_AND_REPLACE) as connection:
            with open(csv_file, "r", encoding="utf-8-sig") as f:
                header = f.readline().strip().split(",")
            
            table_def = TableDefinition(table_name="Extract")
            for col in header:
                table_def.add_column(col, SqlType.text())
            connection.catalog.create_table(table_def)

            with Inserter(connection, table_def) as inserter:
                inserter.add_rows_from_csv(csv_file)
                inserter.execute()

    file_size = os.path.getsize(hyper_name)
    print(f"📦 Size of {hyper_name}: {file_size} bytes")

    print(f"🚀 Uploading {hyper_name} to Tableau staging...")

    upload_req = requests.post(
        f"{BASE_URL}/sites/{site_id}/fileUploads",
        headers={"X-Tableau-Auth": auth_token}
    )

    if upload_req.status_code != 200:
        print(f"🔥 Upload session failed for {csv_file}: {upload_req.status_code}")
        continue

    upload_id = upload_req.json()["fileUpload"]["uploadSessionId"]

    with open(hyper_name, 'rb') as f:
        upload_resp = requests.put(
            f"{BASE_URL}/sites/{site_id}/fileUploads/{upload_id}",
            data=f,
            headers={
                "X-Tableau-Auth": auth_token,
                "Content-Type": "application/octet-stream"
            }
        )

    if upload_resp.status_code != 200:
        print(f"🔥 Upload failed for {hyper_name}: Status {upload_resp.status_code}")
        print(f"🔍 Raw response: {upload_resp.text if upload_resp.text else '[No response body]'}")
        continue

    print(f"✅ Uploaded {hyper_name} to staging")

    # Optional: You could publish here or do it later

# Sign out
requests.post(
    f"{BASE_URL}/auth/signout",
    headers={"X-Tableau-Auth": auth_token}
)
print("\ud83d\udeaa Signed out of Tableau")
