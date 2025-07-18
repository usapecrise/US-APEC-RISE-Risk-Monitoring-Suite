import os
import requests
import xml.etree.ElementTree as ET
from tableauhyperapi import HyperProcess, Connection, Telemetry, TableDefinition, SqlType, Inserter, TableName, CreateMode
import pandas as pd
from requests_toolbelt import MultipartEncoder

# 🔑 Load environment variables
TABLEAU_SITE_ID = os.environ["TABLEAU_SITE_ID"]
TABLEAU_USER_ID = os.environ["TABLEAU_USER_ID"]
TABLEAU_PAT_NAME = os.environ["TABLEAU_PAT_NAME"]
TABLEAU_PAT_SECRET = os.environ["TABLEAU_PAT_SECRET"]
TABLEAU_PROJECT_ID = os.environ["TABLEAU_PROJECT_ID"]
TABLEAU_REST_URL = os.environ["TABLEAU_REST_URL"]

# 🚀 Authenticate
def authenticate():
    auth_payload = {
        "credentials": {
            "personalAccessTokenName": TABLEAU_PAT_NAME,
            "personalAccessTokenSecret": TABLEAU_PAT_SECRET,
            "site": {"contentUrl": ""}
        }
    }
    response = requests.post(f"{TABLEAU_REST_URL}/auth/signin", json=auth_payload)
    response.raise_for_status()
    token = response.json()["credentials"]["token"]
    site_id = response.json()["credentials"]["site"]["id"]
    user_id = response.json()["credentials"]["user"]["id"]
    return token, site_id, user_id

# 🧼 Clean up and sign out
def sign_out(token):
    headers = {"X-Tableau-Auth": token}
    requests.post(f"{TABLEAU_REST_URL}/auth/signout", headers=headers)

# 🔁 CSV → HYPER
def convert_csv_to_hyper(csv_file, hyper_file):
    df = pd.read_csv(csv_file)
    with HyperProcess(Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(endpoint=hyper.endpoint, database=hyper_file, create_mode=CreateMode.CREATE_AND_REPLACE) as connection:
            cols = [TableDefinition.Column(col, SqlType.text()) for col in df.columns]
            table_def = TableDefinition(table_name=TableName("Extract", "Extract"), columns=cols)
            connection.catalog.create_table(table_def)
            with Inserter(connection, table_def) as inserter:
                inserter.add_rows(df.values.tolist())
                inserter.execute()

# 📤 Upload + Publish HYPER
def publish_to_tableau(hyper_file, token, site_id):
    headers = {"X-Tableau-Auth": token}
    metadata = f"""
    <tsRequest>
        <datasource name="{hyper_file.stem}" >
            <project id="{TABLEAU_PROJECT_ID}" />
        </datasource>
    </tsRequest>
    """.strip()

    m = MultipartEncoder(
        fields={
            "request_payload": ("", metadata, "text/xml"),
            "tableau_datasource": (hyper_file.name, open(hyper_file, "rb"), "application/octet-stream")
        },
        boundary="----WebKitFormBoundary7MA4YWxkTrZu0gW"
    )

    publish_url = f"{TABLEAU_REST_URL}/sites/{site_id}/datasources?uploadSessionId={hyper_file.stem}&datasourceType=hyper&overwrite=true"
    response = requests.post(publish_url, data=m, headers={
        "X-Tableau-Auth": token,
        "Content-Type": m.content_type
    })

    if response.status_code == 201:
        print(f"✅ Published: {hyper_file.name}")
    else:
        print(f"🔥 Failed to publish {hyper_file.name}: {response.status_code}")
        print(f"🔍 Response: {response.text}")

# 🧠 Main logic
def main():
    print("🚦 Starting Tableau Hyper upload script")
    csv_files = [f for f in os.listdir() if f.endswith(".csv")]
    print(f"🗂️ Matched CSV files: {csv_files}")

    token, site_id, user_id = authenticate()
    print("✅ Tableau auth successful")

    for csv in csv_files:
        hyper = csv.replace(".csv", ".hyper")
        print(f"\n⚙️ Converting {csv} to {hyper}...")
        convert_csv_to_hyper(csv, hyper)
        print(f"📦 Created {hyper} ({os.path.getsize(hyper)} bytes)")
        print(f"📤 Publishing {hyper} to Tableau...")
        publish_to_tableau(Path(hyper), token, site_id)

    sign_out(token)
    print("🚪 Signed out of Tableau")

if __name__ == "__main__":
    from pathlib import Path
    main()
