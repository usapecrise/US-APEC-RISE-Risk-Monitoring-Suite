import os
import glob
import pandas as pd
import xml.etree.ElementTree as ET
import requests
from tableauhyperapi import HyperProcess, Telemetry, Connection, TableDefinition, SqlType, Inserter, CreateMode

# ---🔐 Load env vars
PAT_NAME = os.environ["TABLEAU_PAT_NAME"]
PAT_SECRET = os.environ["TABLEAU_PAT_SECRET"]
SITE_NAME = os.environ["TABLEAU_SITE_NAME"]
SITE_ID = os.environ["TABLEAU_SITE_ID"]
PROJECT_ID = os.environ["TABLEAU_PROJECT_ID"]
TABLEAU_REST_URL = os.environ["TABLEAU_REST_URL"]

print("🚦 Starting Tableau Hyper upload script")

def tableau_auth():
    url = f"{TABLEAU_REST_URL}/api/3.25/auth/signin"
    xml_payload = f"""
        <tsRequest>
            <credentials personalAccessTokenName="{PAT_NAME}" personalAccessTokenSecret="{PAT_SECRET}">
                <site contentUrl="{SITE_NAME}" />
            </credentials>
        </tsRequest>
    """
    headers = {"Content-Type": "application/xml"}
    response = requests.post(url, data=xml_payload.encode("utf-8"), headers=headers)
    response.raise_for_status()
    root = ET.fromstring(response.text)
    token = root.find(".//t:credentials", {"t": "http://tableau.com/api"}).attrib["token"]
    site_id = root.find(".//t:site", {"t": "http://tableau.com/api"}).attrib["id"]
    print("✅ Tableau auth successful")
    return token, site_id

def convert_csv_to_hyper(csv_path, hyper_path):
    df = pd.read_csv(csv_path)
    df.fillna("", inplace=True)
    table_def = TableDefinition(table_name="Extract")
    for col in df.columns:
        table_def.add_column(col, SqlType.text())

    with HyperProcess as hyper:
        with Connection(endpoint=hyper.endpoint, database=hyper_path, create_mode=CreateMode.CREATE_AND_REPLACE) as connection:
            connection.catalog.create_table(table_def)
            with Inserter(connection, table_def) as inserter:
                inserter.add_rows(rows=df.values.tolist())
                inserter.execute()

def initiate_upload(token, site_id):
    url = f"{TABLEAU_REST_URL}/api/3.25/sites/{site_id}/fileUploads"
    headers = {"X-Tableau-Auth": token}
    res = requests.post(url, headers=headers)
    res.raise_for_status()
    upload_session_id = ET.fromstring(res.text).find(".//t:fileUpload", {"t": "http://tableau.com/api"}).attrib["uploadSessionId"]
    return upload_session_id

def upload_file_part(token, site_id, upload_session_id, file_path):
    url = f"{TABLEAU_REST_URL}/api/3.25/sites/{site_id}/fileUploads/{upload_session_id}"
    headers = {"X-Tableau-Auth": token}
    with open(file_path, 'rb') as f:
        response = requests.put(url, data=f, headers=headers)
    response.raise_for_status()

def publish_datasource(token, site_id, upload_session_id, file_name, project_id, datasource_name):
    url = f"{TABLEAU_REST_URL}/api/3.25/sites/{site_id}/datasources?uploadSessionId={upload_session_id}&datasourceType=hyper&overwrite=true"
    xml_payload = f"""
        <tsRequest>
            <datasource name="{datasource_name}">
                <project id="{project_id}" />
            </datasource>
        </tsRequest>
    """
    headers = {
        "X-Tableau-Auth": token,
        "Content-Type": "application/xml"
    }
    response = requests.post(url, data=xml_payload.encode("utf-8"), headers=headers)
    if response.status_code == 201:
        print(f"✅ Published: {datasource_name}")
    else:
        print(f"🔥 Failed to publish {datasource_name}: {response.status_code}")
        print(response.text)

def sign_out(token):
    url = f"{TABLEAU_REST_URL}/api/3.25/auth/signout"
    headers = {"X-Tableau-Auth": token}
    requests.post(url, headers=headers)

def main():
    token, site_id = tableau_auth()

    csv_files = glob.glob("*.csv")
    print(f"🗂️ Matched CSV files: {csv_files}")

    for csv_file in csv_files:
        base_name = os.path.splitext(csv_file)[0]
        hyper_file = f"{base_name}.hyper"
        print(f"⚙️ Converting {csv_file} to {hyper_file}...")
        convert_csv_to_hyper(csv_file, hyper_file)

        print(f"📤 Uploading {hyper_file}...")
        upload_session_id = initiate_upload(token, site_id)
        upload_file_part(token, site_id, upload_session_id, hyper_file)
        publish_datasource(token, site_id, upload_session_id, hyper_file, PROJECT_ID, base_name)

    sign_out(token)
    print("🚪 Signed out of Tableau")

if __name__ == "__main__":
    main()
