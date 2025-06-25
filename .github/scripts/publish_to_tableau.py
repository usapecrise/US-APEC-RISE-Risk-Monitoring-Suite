from tableauhyperapi import HyperProcess, Connection, Telemetry, TableDefinition, SqlType, Inserter, CreateMode, TableName
import pandas as pd

# Step 1: Load data
csv_path = "data/risk_signals.csv"
df = pd.read_csv(csv_path)

# Step 2: Define Hyper table schema
table_name = TableName("Extract", "Risk_Signals")
table_definition = TableDefinition(
    table_name=table_name,
    columns=[
        ("Economy", SqlType.text()),
        ("Workstream", SqlType.text()),
        ("Assumption", SqlType.text()),
        ("Scenario", SqlType.text()),
        ("Justification", SqlType.text()),
        ("Signal Strength", SqlType.text()),
        ("Signal Strength (Numeric)", SqlType.int())
    ]
)

# Step 3: Create .hyper file
with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
    with Connection(endpoint=hyper.endpoint, database="risk_signals.hyper", create_mode=CreateMode.CREATE_AND_REPLACE) as connection:
        connection.catalog.create_schema("Extract")
        connection.catalog.create_table(table_definition)

        # Insert rows
        with Inserter(connection, table_definition) as inserter:
            inserter.add_rows(rows=df.itertuples(index=False))
            inserter.execute()

print("✅ Hyper file created successfully: risk_signals.hyper")

# Step 4: Upload to Tableau (Optional: requires Tableau Server/Online setup)
# This part is NOT included unless you're using TSC (Tableau Server Client).

