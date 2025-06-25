import pandas as pd
from tableauhyperapi import HyperProcess, Connection, TableDefinition, SqlType, Inserter, Telemetry, CreateMode
import os

def create_hyper(df):
    # Output file name
    hyper_name = "risk_signals.hyper"
    
    # Remove existing file if present
    if os.path.exists(hyper_name):
        os.remove(hyper_name)

    # Start Hyper API process
    with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(endpoint=hyper.endpoint, database=hyper_name, create_mode=CreateMode.CREATE_AND_REPLACE) as connection:
            table_name = "Extract"

            # Define schema
            table_definition = TableDefinition(
                table_name=table_name,
                columns=[
                    TableDefinition.Column("Signal", SqlType.text()),
                    TableDefinition.Column("Economy", SqlType.text()),
                    TableDefinition.Column("Risk Category", SqlType.text()),
                    TableDefinition.Column("Scenario Implication", SqlType.text()),
                    TableDefinition.Column("Signal Strength (Numeric)", SqlType.int()),  # ← int for sizing/averages
                    TableDefinition.Column("Date", SqlType.text())
                ]
            )

            connection.catalog.create_table(table_definition)

            # Insert rows
            with Inserter(connection, table_definition) as inserter:
                inserter.add_rows(rows=df.itertuples(index=False, name=None))
                inserter.execute()

    print("✅ .hyper file created: risk_signals.hyper")

# Load and clean CSV
print("📥 Downloading CSV...")
df = pd.read_csv("risk_signals.csv")

# Keep only rows with valid numeric signal strengths (1, 3, 5)
df = df[df["Signal Strength (Numeric)"].isin([1, 3, 5])]

# Ensure the column is integer type
df["Signal Strength (Numeric)"] = df["Signal Strength (Numeric)"].astype(int)

# Optional: Print row count
print(f"📊 Rows loaded: {len(df)}")

# Convert to .hyper
print("🧪 Converting CSV to .hyper...")
create_hyper(df)
