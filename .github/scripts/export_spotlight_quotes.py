import pandas as pd
import re
import language_tool_python

# === CONFIG ===
INPUT_FILE = "Feedback_Form_Data_Long.csv"   # ✅ use the exported long-format file
OUTPUT_FILE = "spotlight_quotes.csv"
MAX_LEN = 250   # truncate to fit Tableau box

# === 1. Load feedback entries ===
df = pd.read_csv(INPUT_FILE)

# === 2. Identify open-ended columns ===
QUOTE_COLUMNS = [
    "Application Examples",
    "Potential Barriers",
    "Sharing Examples",
    "Suggested Improvements"
]

# === 3. Basic cleanup ===
def clean_text(x):
    if pd.isna(x):
        return None
    x = str(x).strip()
    x = re.sub(r"\s+", " ", x)  # collapse whitespace
    if len(x) < 10 or x.lower() in ["no", "n/a", "none", "nil"]:
        return None
    return x

for col in QUOTE_COLUMNS:
    if col in df.columns:   # ✅ safeguard in case column missing
        df[col] = df[col].apply(clean_text)

# === 4. Melt to long format ===
quotes_long = df.melt(
    id_vars=["Organization", "Economy"],
    value_vars=[c for c in QUOTE_COLUMNS if c in df.columns],
    var_name="QuoteType",
    value_name="Quote"
)
quotes_long = quotes_long.dropna(subset=["Quote"]).reset_index(drop=True)

# === 5. Grammar correction with LanguageTool ===
tool = language_tool_python.LanguageTool('en-US')

def polish_quote(text):
    matches = tool.check(text)
    corrected = language_tool_python.utils.correct(text, matches)
    # Ensure punctuation at the end
    if corrected and corrected[-1] not in ".!?":
        corrected += "."
    return corrected

quotes_long["Quote"] = quotes_long["Quote"].apply(polish_quote)

# === 6. Rank and select top 5 ===
quotes_long["Score"] = quotes_long["Quote"].str.len()
top_quotes = quotes_long.sort_values("Score", ascending=False).head(5).copy()
top_quotes = top_quotes.reset_index(drop=True)
top_quotes["Order"] = top_quotes.index + 1

# === 7. Truncate quotes to safe length ===
def shorten(text, max_len=MAX_LEN):
    if len(text) > max_len:
        return text[:max_len].rsplit(" ", 1)[0] + "..."
    return text

top_quotes["Quote"] = top_quotes["Quote"].apply(lambda x: shorten(x, MAX_LEN))

# === 8. Export final Spotlight Quotes ===
spotlight = top_quotes[["Order", "Quote", "Organization", "Economy"]]
spotlight.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")

print("✅ Exported Spotlight Quotes to", OUTPUT_FILE)
print(spotlight)
