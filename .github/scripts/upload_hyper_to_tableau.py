import os
import glob
import time
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
    "OC2.csv": "OC2 Extract",
    "OC3.csv": "OC3 Extract",
    "OC4.csv": "OC4 Extract",
    "OC5.csv": "OC5 Extract",
    "OC6.csv": "OC6 Extract",
    "OC7.csv": "OC7 Extract",
    "KPI_Targets.csv": "KPI Target Reference",
    "Workshop_Master_List.csv": "Workshop Master List",
    "Feedback_Form_Data.csv": "Feedback Form Data",
    "Stakeholder_Reference_List.csv": "Stakeholder Reference List",
    "Stakeholder_Reference_List_long.csv": "Stakeholder Reference List Long",
    "Feedback_Form_Data_Long.csv": "Feedback Form Data Long",
    "word_frequency.csv": "Word Frequency Extract",
    "word_frequency_detailed.csv": "Word Frequency Detailed Extract",
    "sentiment_summary.csv": "Sentiment Summary Extract",
    "top_phrases.csv": "Top Phrases Extract",
    "risk_signals.csv": "Risk Signals Extract",
    "sentiment_by_question.csv": "Sentiment By Question",
    "risk_assumption.csv": "Risk Assumptions",
    "policy_reform_assumption.csv": "Policy Reform Assumptions",
    "media_log.csv": "Media Log",
    "feedback_policy_assumption.csv": "Feedback Policy Assumption",
    "feedback_assumption.csv": "Feedback Assumptions",
    "cost_share_assumption.csv": "Cost Share Assumption",
    "attendance_records.csv": "Attendance Records",
    "attendance_continuity_assumption.csv": "Attendance Continuity Assumption",
    "attendance_assumption.csv": "Attendance Assumption",
    "assumptions_status.csv": "Assumptions Status",
    "Map_Data.csv": "Activity Footprint",
    "Monitoring_System.csv": "Monitoring System",
    "person_hours.csv": "Person Hours",
    "spotlight_quotes.csv": "Spotlight Quotes",
    "Data_Quality.csv": "Data Quality",
    "Data_Security.csv": "Data Security",
    "assumptions_summary.csv": "Assumptions Summary",
    "assumptions_status_cards.csv": "Assumptions Status Cards",
    "assumptions_breakdown.csv": "Assumptions Breakdown",
    "assumptions_evidence.csv": "Assumptions Evidence"
}

# ── CONVERT CSV TO HYPER ───────────────────────────────
def convert_csv_to_hyper(csv_path: str, hyper_path: str):
    df = pd.read_csv(csv_path)

    def map_dtype(dtype):
        if pd.api.types.is_integer_dtype(dtype):
            return SqlType.int()
        elif pd.api.types.is_float_dtype(dtype):
            return SqlType.double()
        else:
            return SqlType.text()

    table_def = TableDefinition(table_name=TableName("Extract"))
    for col in df.columns:
        sql_type = map_dtype(df[col].dtype)
        table_def.add_column(col, sql_type)
        if sql_type == SqlType.text():
            df[col] = df[col].astype(str).fillna("")

    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(
            endpoint=hyper.endpoint,
            database=hyper_path,
            create_mode=CreateMode.CREATE_AND_REPLACE
        ) as conn:
            conn.catalog.create_table(table_def)
            with Inserter(conn, table_def) as inserter:
                inserter.add_rows(df.itertuples(index=False, name=None))
                inserter.execute()

    print(f"📦 Created {hyper_path} ({os.path.getsize(hyper_path)} bytes)")

# ── MAIN EXECUTION ─────────────────────────────────────
def main():
    csv_files = glob.glob("*.csv")
    print("🗂️ Found CSVs:", csv_files)

    auth = TSC.PersonalAccessTokenAuth(PAT_NAME, PAT_SECRET, SITE_NAME)
    server = TSC.Server(TABLEAU_SERVER, use_server_version=True)

    for csv_file in csv_files:
        if csv_file not in EXTRACT_NAME_MAP:
            print(f"⚠️ Skipping unrecognized file: {csv_file}")
            continue

        extract_name = EXTRACT_NAME_MAP[csv_file]
        hyper_path = f"{os.path.splitext(csv_file)[0]}.hyper"

        print(f"🔄 Converting {csv_file} → {hyper_path}")
        convert_csv_to_hyper(csv_file, hyper_path)

        ds_item = TSC.DatasourceItem(project_id=PROJECT_ID, name=extract_name)

        for attempt in range(3):
            try:
                with server.auth.sign_in(auth):
                    print(f"📤 Publishing '{hyper_path}' as '{extract_name}' (Attempt {attempt+1})")
                    published_ds = server.datasources.publish(
                        ds_item,
                        hyper_path,
                        mode=TSC.Server.PublishMode.Overwrite
                    )
                    print(f"✅ Overwrote extract: '{extract_name}' (Datasource ID: {published_ds.id})")
                    break
            except Exception as e:
                if attempt < 2:
                    print(f"⚠️ Publish failed for {extract_name}, retrying in 5s... Error: {e}")
                    time.sleep(5)
                else:
                    raise

    print("✅ Finished uploading extracts. Dashboards will auto-refresh.")

if __name__ == "__main__":
    main()
