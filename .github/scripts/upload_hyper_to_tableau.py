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
    csv_files = glob.glob("*.csv")
    hyper_files = []
    print("🗂️ Found CSVs:", csv_files)

    for csv in csv_files:
        base = os.path.splitext(csv)[0]
        hyper = f"{base}.hyper"
        convert_csv_to_hyper(csv, hyper)
        hyper_files.append((base, hyper))

    auth = TSC.PersonalAccessTokenAuth(PAT_NAME, PAT_SECRET, SITE_NAME)
    server = TSC.Server(TABLEAU_SERVER, use_server_version=True)

    with server.auth.sign_in(auth):
        for base, hyper in hyper_files:
            print(f"📤 Publishing {hyper} as '{base}' into project ID {PROJECT_ID}")

            # Force it to be published as an extract with Overwrite
            ds_item = TSC.DatasourceItem(project_id=PROJECT_ID, name=base)
            published_ds = server.datasources.publish(
                ds_item,
                hyper,
                mode=TSC.Server.PublishMode.Overwrite,
                connection_credentials=None  # force no embedded live connection
            )

            print(f"✅ Overwrote extract: '{base}' (Datasource ID: {published_ds.id})")

    print("🚪 Finished upload process.")

if __name__ == "__main__":
    main()


