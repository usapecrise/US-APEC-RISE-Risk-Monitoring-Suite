import pandas as pd
from tableauhyperapi import HyperProcess, Connection, TableDefinition, SqlType, Inserter, Telemetry, CreateMode
import os

def create_hyper(df):
    # Ensure correct data types
    df["Signal Strength (Numeric)"] = df["Signal Strength (Numeric)"].astype(int)

    # Define output path
    hyper_name = "risk_signals.hyper"
    if os.path.exists(hyper_name):
        os.remove(hyper_name)

    # Start the Hyper process
    with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(endpoint=hyper.endpoint, database=hyper_name, create_mode=CreateMode.CREATE_AND_REPLACE) as connection:

            # Define table name and schema
            table_name = "Extract"
            table_definition = TableDefinition(
                table_name=table_name,
                columns=[
                    TableDefinition.Column("Signal", SqlType.text()),
                    TableDefinition.Column("Economy", SqlType.text()),
                    TableDefinition.Column("Risk Category", SqlType.text()),
                    TableDefinition.Column("Scenario Implication", SqlType.text()),
                    TableDefinition.Column("Signal Strength (Numeric)", SqlType.int()),
                    TableDefinition.Column("Date", SqlType.text())
                ]
            )

            connection.catalog.create_table(table_definition)

            # Insert data
            with Inserter(connection, table_definition) as inserter:
                inserter.add_rows(rows=df.itertuples(index=False, name=None))
                inserter.execute()

    print(f"✅ .hyper file created: {hyper_name}")

# Load CSV data
print("📥 Downloading CSV...")
df = pd.read_csv("data/risk_signals.csv")

# Convert and export to Hyper
print("🧪 Converting CSV to .hyper...")
create_hyper(df)
