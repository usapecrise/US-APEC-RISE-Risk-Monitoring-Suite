import pandas as pd
import re
import language_tool_python

# === CONFIG ===
INPUT_FILE = "Feedback_Form_Data_Long.csv"
OUTPUT_FILE = "spotlight_quotes.csv"
MAX_LEN = 250

# === 1. Load feedback entries ===
df = pd.read_csv(INPUT_FILE)

# === 2. Identify open-ended columns ===
QUOTE_COLUMNS = [
    "Application Examples",
    "Potential Barriers",
    "Sharing Examples",
    "Suggested Improvements"
]

# === 3. Cleanup function ===
def clean_text(x):
    if pd.isna(x):
        return None
    x = str(x).strip()
    x = re.sub(r"\s+", " ", x)
    if len(x) < 10 or x.lower() in ["no", "n/a", "none", "nil"]:
        return None
    return x

for col in QUOTE_COLUMNS:
    if col in df.columns:
        df[col] = df[col].apply(clean_text)

# === 4. Melt to long format ===
quotes_long = df.melt(
    id_vars=["Workshop Title", "Organization", "Economy"],
    value_vars=[c for c in QUOTE_COLUMNS if c in df.columns],
    var_name="QuoteType",
    value_name="Quote"
)
quotes_long = quotes_long.dropna(subset=["Quote"]).reset_index(drop=True)

# === 5. Deduplicate quotes (within each workshop) ===
quotes_long = quotes_long.drop_duplicates(
    subset=["Workshop Title", "Quote"]
).reset_index(drop=True)

# === 6. Grammar correction ===
tool = language_tool_python.LanguageTool('en-US')

def polish_quote(text):
    matches = tool.check(text)
    corrected = language_tool_python.utils.correct(text, matches)
    if corrected and corrected[-1] not in ".!?":
        corrected += "."
    return corrected

quotes_long["Quote"] = quotes_long["Quote"].apply(polish_quote)

# === 7. Select up to 5 quotes per workshop ===
def shorten(text, max_len=MAX_LEN):
    if len(text) > max_len:
        return text[:max_len].rsplit(" ", 1)[0] + "..."
    return text

quotes_long["Score"] = quotes_long["Quote"].str.len()

spotlight_all = []
for workshop, group in quotes_long.groupby("Workshop Title"):
    # Prefer diversity across categories
    selected = []
    for cat in ["Application Examples", "Potential Barriers", "Sharing Examples", "Suggested Improvements"]:
        subset = group[group["QuoteType"] == cat]
        if not subset.empty:
            selected.append(subset.sort_values("Score", ascending=False).iloc[0])
    # Fill remaining slots with other unique top-scoring quotes
    if len(selected) < 5:
        extra = group.sort_values("Score", ascending=False)
        for _, row in extra.iterrows():
            if len(selected) >= 5:
                break
            if row["Quote"] not in [s["Quote"] for s in selected]:
                selected.append(row)
    # Convert back to DataFrame
    workshop_quotes = pd.DataFrame(selected).reset_index(drop=True)
    workshop_quotes["Order"] = workshop_quotes.index + 1
    workshop_quotes["Quote"] = workshop_quotes["Quote"].apply(lambda x: shorten(x, MAX_LEN))
    spotlight_all.append(workshop_quotes)

spotlight = pd.concat(spotlight_all, ignore_index=True)

# === 8. Export ===
spotlight = spotlight[["Workshop Title", "Order", "Quote", "Organization", "Economy"]]
spotlight.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")

print("✅ Exported Spotlight Quotes per workshop to", OUTPUT_FILE)
print(spotlight.head(15))
