import os
import glob
import pandas as pd
import xml.etree.ElementTree as ET
import requests
from tableauhyperapi import (
    HyperProcess,
    Telemetry,
    Connection,
    TableDefinition,
    SqlType,
    Inserter,
    CreateMode
)

# ──🔐 ENVIRONMENT VARIABLES ──────────────────────────────────────────────
PAT_NAME         = os.environ["TABLEAU_PAT_NAME"]
PAT_SECRET       = os.environ["TABLEAU_PAT_SECRET"]
SITE_NAME        = os.environ["TABLEAU_SITE_NAME"]      # e.g. 'thecadmusgrouponline'
PROJECT_ID       = os.environ["TABLEAU_PROJECT_ID"]     # your Tableau Project UUID
TABLEAU_REST_URL = os.environ["TABLEAU_REST_URL"]       # e.g. 'https://prod-useast-a.online.tableau.com'

NS = {"t": "http://tableau.com/api"}

def tableau_auth():
    """Authenticate: return (token, site_id)."""
    url = f"{TABLEAU_REST_URL}/api/3.25/auth/signin"
    xml_body = f"""
      <tsRequest>
        <credentials personalAccessTokenName="{PAT_NAME}"
                     personalAccessTokenSecret="{PAT_SECRET}">
          <site contentUrl="{SITE_NAME}" />
        </credentials>
      </tsRequest>
    """.strip()

    r = requests.post(
        url,
        data=xml_body.encode("utf-8"),
        headers={"Content-Type": "application/xml", "Accept": "application/xml"},
    )
    r.raise_for_status()
    root = ET.fromstring(r.text)
    token   = root.find(".//t:credentials", NS).attrib["token"]
    site_id = root.find(".//t:site",        NS).attrib["id"]
    print("✅ Authenticated to Tableau Cloud")
    return token, site_id

def convert_csv_to_hyper(csv_path, hyper_path):
    """Convert a CSV to a Hyper extract (all columns TEXT)."""
    df = pd.read_csv(csv_path).fillna("")
    table_def = TableDefinition(table_name="Extract")
    for col in df.columns:
        table_def.add_column(col, SqlType.text())

    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(
            endpoint=hyper.endpoint,
            database=hyper_path,
            create_mode=CreateMode.CREATE_AND_REPLACE
        ) as conn:
            conn.catalog.create_table(table_def)
            with Inserter(conn, table_def) as inserter:
                inserter.add_rows(rows=df.values.tolist())
                inserter.execute()
    size = os.path.getsize(hyper_path)
    print(f"📦 Created {hyper_path} ({size} bytes)")

def initiate_upload(token, site_id):
    """Start upload session; return uploadSessionId."""
    url = f"{TABLEAU_REST_URL}/api/3.25/sites/{site_id}/fileUploads"
    r = requests.post(url, headers={"X-Tableau-Auth": token, "Accept": "application/xml"})
    r.raise_for_status()
    upload_id = ET.fromstring(r.text).find(".//t:fileUpload", NS).attrib["uploadSessionId"]
    return upload_id

def upload_file_part(token, site_id, upload_id, file_path):
    """PUT the entire .hyper file in one chunk (with proper headers)."""
    url = f"{TABLEAU_REST_URL}/api/3.25/sites/{site_id}/fileUploads/{upload_id}"
    # Read into memory so requests can set Content-Length
    data = open(file_path, "rb").read()
    r = requests.put(
        url,
        data=data,
        headers={
            "X-Tableau-Auth": token,
            "Content-Type": "application/octet-stream",
            "Accept": "application/xml",
        },
    )
    r.raise_for_status()

def publish_datasource(token, site_id, upload_id, datasource_name):
    """Publish the previously‑uploaded file as a data source."""
    url = (
        f"{TABLEAU_REST_URL}/api/3.25/sites/{site_id}/datasources"
        f"?uploadSessionId={upload_id}&datasourceType=hyper&overwrite=true"
    )
    xml_body = f"""
      <tsRequest>
        <datasource name="{datasource_name}">
          <project id="{PROJECT_ID}" />
        </datasource>
      </tsRequest>
    """.strip()
    r = requests.post(
        url,
        data=xml_body.encode("utf-8"),
        headers={
            "X-Tableau-Auth": token,
            "Content-Type": "application/xml",
            "Accept": "application/xml",
        },
    )
    if r.status_code == 201:
        print(f"✅ Published {datasource_name}")
    else:
        print(f"🔥 Publish failed for {datasource_name}: {r.status_code}")
        print(r.text)

def sign_out(token):
    url = f"{TABLEAU_REST_URL}/api/3.25/auth/signout"
    requests.post(url, headers={"X-Tableau-Auth": token})
    print("🚪 Signed out of Tableau")

def main():
    token, site_id = tableau_auth()

    csv_files = glob.glob("*.csv")
    print("🗂️  CSV files found:", csv_files)

    for csv_file in csv_files:
        base = os.path.splitext(os.path.basename(csv_file))[0]
        hyper = f"{base}.hyper"

        print(f"\n⚙️  Converting {csv_file} → {hyper}")
        convert_csv_to_hyper(csv_file, hyper)

        print(f"📤 Initiating upload for {hyper}")
        upload_id = initiate_upload(token, site_id)

        print(f"📦 Uploading {hyper}")
        upload_file_part(token, site_id, upload_id, hyper)

        print(f"🔗 Publishing '{base}'")
        publish_datasource(token, site_id, upload_id, base)

    sign_out(token)

if __name__ == "__main__":
    main()

