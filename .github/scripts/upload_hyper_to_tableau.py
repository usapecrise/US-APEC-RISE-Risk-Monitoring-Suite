import os
import glob
import pandas as pd
from tableauhyperapi import (
    HyperProcess, Telemetry, Connection, TableDefinition,
    SqlType, Inserter, CreateMode
)
import tableauserverclient as TSC

# ── CONFIG ──────────────────────────────────────────────
PAT_NAME       = os.environ["TABLEAU_PAT_NAME"]
PAT_SECRET     = os.environ["TABLEAU_PAT_SECRET"]
SITE_NAME      = os.environ["TABLEAU_SITE_NAME"]
PROJECT_ID     = os.environ["TABLEAU_PROJECT_ID"]
TABLEAU_SERVER = os.environ["TABLEAU_REST_URL"]

# ── FIXED NAME FOR EXTRACT ─────────────────────────────
FIXED_EXTRACT_NAME = "OT1 Extract"      # 👈 This is the name Tableau Cloud will use
INPUT_CSV_FILENAME = "OT1.csv"          # 👈 CSV file to convert and upload
OUTPUT_HYPER_FILE  = "OT1.hyper"        # 👈 Temp file name used locally

# ── CONVERT CSV TO HYPER ───────────────────────────────
def convert_csv_to_hyper(csv_path: str, hyper_path: str):
    df = pd.read_csv(csv_path).fillna("").astype(str)
    table_def = TableDefinition(table_name="Extract")
    for col in df.columns:
        table_def.add_column(col, SqlType.text())

    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(endpoint=hyper.endpoint, database=hyper_path, create_mode=CreateMode.CREATE_AND_REPLACE) as conn:
            conn.catalog.create_table(table_def)
            with Inserter(conn, table_def) as inserter:
                inserter.add_rows(rows=df.values.tolist())
                inserter.execute()

    print(f"📦 Created {hyper_path} ({os.path.getsize(hyper_path)} bytes)")

# ── PUBLISH TO TABLEAU CLOUD ───────────────────────────
def main():
    if not os.path.exists(INPUT_CSV_FILENAME):
        print(f"❌ CSV not found: {INPUT_CSV_FILENAME}")
        return

    # Step 1: Convert to .hyper
    print(f"🔄 Converting {INPUT_CSV_FILENAME} → {OUTPUT_HYPER_FILE}")
    convert_csv_to_hyper(INPUT_CSV_FILENAME, OUTPUT_HYPER_FILE)

    # Step 2: Publish to Tableau
    auth = TSC.PersonalAccessTokenAuth(PAT_NAME, PAT_SECRET, SITE_NAME)
    server = TSC.Server(TABLEAU_SERVER, use_server_version=True)

    with server.auth.sign_in(auth):
        print(f"📤 Publishing '{OUTPUT_HYPER_FILE}' as '{FIXED_EXTRACT_NAME}' into project ID {PROJECT_ID}")

        ds_item = TSC.DatasourceItem(project_id=PROJECT_ID, name=FIXED_EXTRACT_NAME)
        ds_item.connection_credentials = None  # Prevents embedding of live creds

        published_ds = server.datasources.publish(
            ds_item,
            OUTPUT_HYPER_FILE,
            mode=TSC.Server.PublishMode.Overwrite
        )

        print(f"✅ Overwrote extract: '{FIXED_EXTRACT_NAME}' (Datasource ID: {published_ds.id})")

    print("🚪 Finished upload process.")

if __name__ == "__main__":
    main()

