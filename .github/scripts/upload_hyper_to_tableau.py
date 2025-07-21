import os
import glob
import pandas as pd
import xml.etree.ElementTree as ET
import requests
from tableauhyperapi import (
    HyperProcess,
    Connection,
    TableDefinition,
    SqlType,
    Inserter,
    CreateMode,
)

# ──🔐 Load creds from env vars (GitHub Secrets) ───────────────────────────────────
PAT_NAME        = os.environ["TABLEAU_PAT_NAME"]
PAT_SECRET      = os.environ["TABLEAU_PAT_SECRET"]
SITE_NAME       = os.environ["TABLEAU_SITE_NAME"]     # e.g. 'thecadmusgrouponline'
PROJECT_ID      = os.environ["TABLEAU_PROJECT_ID"]    # project UUID in Tableau
TABLEAU_REST_URL= os.environ["TABLEAU_REST_URL"]      # e.g. 'https://prod-useast-a.online.tableau.com'

# Namespace map for XML parsing
NS = {"t": "http://tableau.com/api"}

def tableau_auth():
    """Authenticate with Tableau Cloud, return (token, site_id)."""
    url = f"{TABLEAU_REST_URL}/api/3.25/auth/signin"
    xml = f"""
      <tsRequest>
        <credentials personalAccessTokenName="{PAT_NAME}"
                     personalAccessTokenSecret="{PAT_SECRET}">
          <site contentUrl="{SITE_NAME}" />
        </credentials>
      </tsRequest>
    """.strip()
    res = requests.post(url, data=xml.encode("utf-8"), headers={"Content-Type": "application/xml"})
    res.raise_for_status()
    root = ET.fromstring(res.text)
    token   = root.find(".//t:credentials", NS).attrib["token"]
    site_id = root.find(".//t:site",       NS).attrib["id"]
    print("✅ Authenticated to Tableau Cloud")
    return token, site_id

def convert_csv_to_hyper(csv_path, hyper_path):
    """Read CSV, create a .hyper with all columns as TEXT."""
    df = pd.read_csv(csv_path).fillna("")
    table_def = TableDefinition(table_name="Extract")
    for col in df.columns:
        table_def.add_column(col, SqlType.text())

    with HyperProcess() as hyper:
        with Connection(endpoint=hyper.endpoint,
                        database=hyper_path,
                        create_mode=CreateMode.CREATE_AND_REPLACE) as conn:
            conn.catalog.create_table(table_def)
            with Inserter(conn, table_def) as inserter:
                inserter.add_rows(rows=df.values.tolist())
                inserter.execute()
    size = os.path.getsize(hyper_path)
    print(f"📦 Created {hyper_path} ({size} bytes)")

def initiate_upload(token, site_id):
    """Start a file upload session, return uploadSessionId."""
    url = f"{TABLEAU_REST_URL}/api/3.25/sites/{site_id}/fileUploads"
    res = requests.post(url, headers={"X-Tableau-Auth": token})
    res.raise_for_status()
    root = ET.fromstring(res.text)
    upload_id = root.find(".//t:fileUpload", NS).attrib["uploadSessionId"]
    return upload_id

def upload_file_part(token, site_id, upload_id, file_path):
    """PUT the binary .hyper to the upload session endpoint."""
    url = f"{TABLEAU_REST_URL}/api/3.25/sites/{site_id}/fileUploads/{upload_id}"
    with open(file_path, "rb") as f:
        res = requests.put(url, data=f, headers={
            "X-Tableau-Auth": token,
            "Content-Type": "application/octet-stream"
        })
    res.raise_for_status()

def publish_datasource(token, site_id, upload_id, datasource_name):
    """Publish the uploaded file as a datasource in your project."""
    url = (
        f"{TABLEAU_REST_URL}/api/3.25/sites/{site_id}/datasources"
        f"?uploadSessionId={upload_id}&datasourceType=hyper&overwrite=true"
    )
    xml = f"""
      <tsRequest>
        <datasource name="{datasource_name}">
          <project id="{PROJECT_ID}" />
        </datasource>
      </tsRequest>
    """.strip()
    res = requests.post(url, data=xml.encode("utf-8"), headers={
        "X-Tableau-Auth": token,
        "Content-Type": "application/xml"
    })
    if res.status_code == 201:
        print(f"✅ Published {datasource_name}")
    else:
        print(f"🔥 Publish failed for {datasource_name}: {res.status_code}")
        print(res.text)

def sign_out(token):
    """Invalidate the Tableau session token."""
    url = f"{TABLEAU_REST_URL}/api/3.25/auth/signout"
    requests.post(url, headers={"X-Tableau-Auth": token})
    print("🚪 Signed out of Tableau")

def main():
    token, site_id = tableau_auth()

    csv_files = glob.glob("*.csv")
    print("🗂️  CSVs found:", csv_files)
    for csv_file in csv_files:
        base = os.path.splitext(csv_file)[0]
        hyper = f"{base}.hyper"

        print(f"\n⚙️  Converting {csv_file} → {hyper}")
        convert_csv_to_hyper(csv_file, hyper)

        print(f"📤 Uploading and publishing {hyper}")
        upload_id = initiate_upload(token, site_id)
        upload_file_part(token, site_id, upload_id, hyper)
        publish_datasource(token, site_id, upload_id, base)

    sign_out(token)

if __name__ == "__main__":
    main()
