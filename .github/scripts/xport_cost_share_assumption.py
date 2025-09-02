import pandas as pd

INPUT_FILE = "OT5 Private Sector Resources-Grid view.csv"
OUTPUT_FILE = "cost_share_assumption.csv"

def classify_status(total_amount: float, num_events: int) -> str:
    """Apply hybrid classification rule."""
    if total_amount >= 10000 or num_events >= 2:
        return "optimistic"
    elif total_amount > 0:
        return "baseline"
    else:
        return "pessimistic"

def main():
    try:
        df = pd.read_csv(INPUT_FILE)
    except FileNotFoundError:
        print(f"⚠️ File not found: {INPUT_FILE}")
        return
    if df.empty:
        print("⚠️ No cost-share data found")
        return

    # Normalize dates
    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        last_date = df["Date"].max().strftime("%Y-%m-%d")
    else:
        last_date = pd.Timestamp.today().strftime("%Y-%m-%d")

    # Filter to local contributions only
    if "Resource Origin" in df.columns:
        df = df[df["Resource Origin"].astype(str).str.lower() == "local"]

    if df.empty:
        print("⚠️ No local (host economy) contributions found")
        return

    # Group by economy (and fiscal year if available)
    group_cols = ["Economy"]
    if "Fiscal Year" in df.columns:
        group_cols.append("Fiscal Year")

    grouped = df.groupby(group_cols).agg(
        total_amount=("Amount", "sum"),
        num_events=("Engagement", "nunique")
    ).reset_index()

    records = []
    for _, row in grouped.iterrows():
        economy = row["Economy"]
        fiscal_year = row.get("Fiscal Year", "N/A")
        total_amount = row["total_amount"] if pd.notnull(row["total_amount"]) else 0
        num_events = row["num_events"] if pd.notnull(row["num_events"]) else 0

        status = classify_status(total_amount, num_events)

        records.append({
            "assumption": "Responsible local ownership",
            "monitoring_tool": "cost_share",
            "economy": economy,
            "date": last_date,
            "signal": f"${total_amount:,.0f} contributed across {num_events} event(s) (FY{fiscal_year})",
            "status": status,
            "notes": "Host economy contributions (cash or in-kind) only"
        })

    out_df = pd.DataFrame(records)
    out_df.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Cost-share assumption saved → {OUTPUT_FILE} ({len(out_df)} rows)")

if __name__ == "__main__":
    main()
