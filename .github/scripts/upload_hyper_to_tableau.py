import os
import glob
import pandas as pd
from tableauhyperapi import (
    HyperProcess, Telemetry, Connection, TableDefinition,
    SqlType, Inserter, CreateMode, TableName
)
import tableauserverclient as TSC

# ── CONFIG ──────────────────────────────────────────────
PAT_NAME       = os.environ["TABLEAU_PAT_NAME"]
PAT_SECRET     = os.environ["TABLEAU_PAT_SECRET"]
SITE_NAME      = os.environ["TABLEAU_SITE_NAME"]
PROJECT_ID     = os.environ["TABLEAU_PROJECT_ID"]
TABLEAU_SERVER = os.environ["TABLEAU_REST_URL"]

# ── FIXED MAPPINGS: CSV → EXTRACT NAME ──────────────────
EXTRACT_NAME_MAP = {
    "OT1.csv": "OT1 Extract",
    "OT2.csv": "OT2 Extract",
    "OT3.csv": "OT3 Extract",
    "OT4.csv": "OT4 Extract",
    "OT5.csv": "OT5 Extract",
    "OC1.csv": "OC1 Extract",
    "OC4.csv": "OC4 Extract",
    "OC5.csv": "OC5 Extract",
    "OC6.csv": "OC6 Extract",
    "OC7.csv": "OC7 Extract",
    "KPI_targets.csv": "KPI Target Reference"
}

# ── CONVERT CSV TO HYPER ───────────────────────────────
def convert_csv_to_hyper(csv_path: str, hyper_path: str):
    df = pd.read_csv(csv_path)

    # Use explicit types based on df dtypes
    def map_dtype(dtype):
        if pd.api.types.is_integer_dtype(dtype):
            return SqlType.int()
        elif pd.api.types.is_float_dtype(dtype):
            return SqlType.double()
        else:
            return SqlType.text()

    table_def = TableDefinition(table_name=TableName("Extract"))
    for col in df.columns:
        table_def.add_column(col, map_dtype(df[col].dtype))

    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(endpoint=hyper.endpoint, database=hyper_path, create_mode=CreateMode.CREATE_AND_REPLACE) as conn:
            conn.catalog.create_table(table_def)
            with Inserter(conn, table_def) as inserter:
                inserter.add_rows(rows=df.itertuples(index=False, name=None))
                inserter.execute()

    print(f"📦 Created {hyper_path} ({os.path.getsize(hyper_path)} bytes)")

# ── PUBLISH TO TABLEAU CLOUD ───────────────────────────
def main():
    csv_files = glob.glob("*.csv")
    print("🗂️ Found CSVs:", csv_files)

    auth = TSC.PersonalAccessTokenAuth(PAT_NAME, PAT_SECRET, SITE_NAME)
    server = TSC.Server(TABLEAU_SERVER, use_server_version=True)

    with server.auth.sign_in(auth):
        for csv_file in csv_files:
            if csv_file not in EXTRACT_NAME_MAP:
                print(f"⚠️ Skipping unrecognized file: {csv_file}")
                continue

            extract_name = EXTRACT_NAME_MAP[csv_file]
            hyper_path = f"{os.path.splitext(csv_file)[0]}.hyper"

            print(f"🔄 Converting {csv_file} → {hyper_path}")
            convert_csv_to_hyper(csv_file, hyper_path)

            print(f"📤 Publishing '{hyper_path}' as '{extract_name}' into project ID {PROJECT_ID}")
            ds_item = TSC.DatasourceItem(project_id=PROJECT_ID, name=extract_name)
            ds_item.connection_credentials = None

            published_ds = server.datasources.publish(
                ds_item,
                hyper_path,
                mode=TSC.Server.PublishMode.Overwrite
            )

            print(f"✅ Overwrote extract: '{extract_name}' (Datasource ID: {published_ds.id})")

    print("🚪 Finished uploading all extracts.")

if __name__ == "__main__":
    main()

