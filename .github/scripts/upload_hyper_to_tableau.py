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

# ── CONFIG ─────────────────────────────────────────────────────────────────────
API_VERSION      = "3.21"
PAT_NAME         = os.environ["TABLEAU_PAT_NAME"]
PAT_SECRET       = os.environ["TABLEAU_PAT_SECRET"]
SITE_NAME        = os.environ["TABLEAU_SITE_NAME"]      # e.g. 'thecadmusgrouponline'
PROJECT_ID       = os.environ["TABLEAU_PROJECT_ID"]     # your Tableau Project UUID
TABLEAU_REST_URL = os.environ["TABLEAU_REST_URL"]       # e.g. 'https://prod-useast-a.online.tableau.com'
NS               = {"t": "http://tableau.com/api"}

# ── AUTH ────────────────────────────────────────────────────────────────────────
def tableau_auth():
    url = f"{TABLEAU_REST_URL}/api/{API_VERSION}/auth/signin"
    xml = f"""
      <tsRequest>
        <credentials personalAccessTokenName="{PAT_NAME}"
                     personalAccessTokenSecret="{PAT_SECRET}">
          <site contentUrl="{SITE_NAME}" />
        </credentials>
      </tsRequest>
    """.strip()
    r = requests.post(
        url,
        data=xml.encode("utf-8"),
        headers={"Content-Type": "application/xml"}
    )
    r.raise_for_status()
    tree = ET.fromstring(r.text)
    token   = tree.find(".//t:credentials", NS).attrib["token"]
    site_id = tree.find(".//t:site",        NS).attrib["id"]
    print("✅ Authenticated")
    return token, site_id

# ── CSV → HYPER ────────────────────────────────────────────────────────────────
def convert_csv_to_hyper(csv_path, hyper_path):
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
            with Inserter(conn, table_def) as ins:
                ins.add_rows(rows=df.values.tolist())
                ins.execute()
    size = os.path.getsize(hyper_path)
    print(f"📦 {hyper_path} ({size} bytes)")

# ── UPLOAD SESSION ─────────────────────────────────────────────────────────────
def initiate_upload(token, site_id):
    url = f"{TABLEAU_REST_URL}/api/{API_VERSION}/sites/{site_id}/fileUploads"
    r = requests.post(url, headers={"X-Tableau-Auth": token})
    r.raise_for_status()
    return ET.fromstring(r.text).find(".//t:fileUpload", NS).attrib["uploadSessionId"]

def upload_file_part(token, site_id, upload_id, file_path):
    url = f"{TABLEAU_REST_URL}/api/{API_VERSION}/sites/{site_id}/fileUploads/{upload_id}"
    data = open(file_path, "rb").read()  # so Content-Length is set
    r = requests.put(
        url,
        data=data,
        headers={
            "X-Tableau-Auth": token,
            "Content-Type": "application/octet-stream"
        },
    )
    r.raise_for_status()

# ── PUBLISH ─────────────────────────────────────────────────────────────────────
def publish_datasource(token, site_id, upload_id, datasource_name):
    url = (
        f"{TABLEAU_REST_URL}/api/{API_VERSION}/sites/{site_id}/datasources"
        f"?uploadSessionId={upload_id}&datasourceType=hyper&overwrite=true"
    )
    xml = f"""
      <tsRequest>
        <datasource name="{datasource_name}">
          <project id="{PROJECT_ID}" />
        </datasource>
      </tsRequest>
    """.strip()
    r = requests.post(
        url,
        data=xml.encode("utf-8"),
        headers={
            "X-Tableau-Auth": token,
            "Content-Type": "application/xml"
        },
    )
    if r.status_code == 201:
        print(f"✅ Published {datasource_name}")
    else:
        print(f"🚨 Publish failed {datasource_name}: {r.status_code}")
        print(r.text)

# ── SIGN OUT ───────────────────────────────────────────────────────────────────
def sign_out(token):
    url = f"{TABLEAU_REST_URL}/api/{API_VERSION}/auth/signout"
    requests.post(url, headers={"X-Tableau-Auth": token})
    print("🚪 Signed out")

# ── MAIN ────────────────────────────────────────────────────────────────────────
def main():
    token, site_id = tableau_auth()
    csvs = glob.glob("*.csv")
    print("🔍 CSVs:", csvs)

    for csv in csvs:
        base = os.path.splitext(csv)[0]
        hyper = f"{base}.hyper"

        print(f"\n⚙️  CSV→HYPER: {csv}")
        convert_csv_to_hyper(csv, hyper)

        print(f"📤 Upload session for {hyper}")
        upid = initiate_upload(token, site_id)

        print(f"🔄 Uploading {hyper}")
        upload_file_part(token, site_id, upid, hyper)

        print(f"🔗 Publishing {base}")
        publish_datasource(token, site_id, upid, base)

    sign_out(token)

if __name__ == "__main__":
    main()
