import pandas as pd
from tableauhyperapi import (
    HyperProcess, Connection, Telemetry,
    TableDefinition, SqlType,
    Inserter, CreateMode, TableName
)

# Step 1: Load CSV
print("📥 Downloading CSV...")
df = pd.read_csv("data/risk_signals.csv")
print(f"📊 Rows loaded: {len(df)}")

# Step 2: Ensure Signal Strength is numeric
df["Signal Strength (Numeric)"] = pd.to_numeric(
    df["Signal Strength (Numeric)"], errors="coerce"
).fillna(0).astype(int)

# Step 3: Create .hyper file
print("🧪 Converting CSV to .hyper...")

def create_hyper(df):
    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(
            endpoint=hyper.endpoint,
            database="risk_signals.hyper",
            create_mode=CreateMode.CREATE_AND_REPLACE
        ) as connection:

            table_def = TableDefinition(table_name=TableName("Extract", "risk_signals"))
            table_def.add_column("Date", SqlType.text())
            table_def.add_column("Economy", SqlType.text())
            table_def.add_column("Headline", SqlType.text())
            table_def.add_column("Signal Strength", SqlType.text())
            table_def.add_column("Signal Strength (Numeric)", SqlType.int())
            table_def.add_column("Scenario", SqlType.text())
            table_def.add_column("Source", SqlType.text())

            connection.catalog.create_table(table_def)

            with Inserter(connection, table_def) as inserter:
                inserter.add_rows(rows=df.itertuples(index=False, name=None))
                inserter.execute()

create_hyper(df)

print("✅ Done publishing to Tableau.")

