import pandas as pd
import re
import openai
import os

# === CONFIG ===
INPUT_FILE = "Feedback_Form_Data_Long.csv"
OUTPUT_FILE = "spotlight_quotes.csv"
MAX_LEN = 250

# === Set up OpenAI ===
openai.api_key = os.getenv("OPENAI_API_KEY")

# === 1. Load feedback entries ===
df = pd.read_csv(INPUT_FILE)

# === 2. Drop non-APEC economies ===
apec_only = df[df["Economy"] != "Other"].copy()

# === 3. Identify open-ended columns ===
QUOTE_COLUMNS = [
    "Application Examples",
    "Potential Barriers",
    "Sharing Examples",
    "Suggested Improvements"
]

# === 4. Clean text ===
def clean_text(x):
    if pd.isna(x):
        return None
    x = str(x).strip()
    x = re.sub(r"\s+", " ", x)
    if len(x) < 10 or x.lower() in ["no", "n/a", "none", "nil"]:
        return None
    return x

for col in QUOTE_COLUMNS:
    if col in apec_only.columns:
        apec_only[col] = apec_only[col].apply(clean_text)

# === 5. Melt into long format ===
quotes_long = apec_only.melt(
    id_vars=["Workshop Title", "Organization", "Economy"],
    value_vars=[c for c in QUOTE_COLUMNS if c in apec_only.columns],
    var_name="QuoteType",
    value_name="Quote"
).dropna(subset=["Quote"]).reset_index(drop=True)

# === 6. Deduplicate within workshop ===
quotes_long = quotes_long.drop_duplicates(
    subset=["Workshop Title", "Quote"]
).reset_index(drop=True)

# === 7. Rewrite quotes with OpenAI ===
def rewrite_quote(text):
    try:
        response = openai.ChatCompletion.create(
            model="gpt-4o-mini",
            messages=[{
                "role": "user",
                "content": (
                    "Rewrite the following workshop feedback as a polished, "
                    "concise quote in natural English, suitable for display "
                    "in a stakeholder-facing report. Keep the original meaning, "
                    "limit to 2 sentences:\n\n" + text
                )
            }],
            temperature=0.5
        )
        polished = response["choices"][0]["message"]["content"].strip()
        if polished and polished[-1] not in ".!?":
            polished += "."
        return polished
    except Exception as e:
        print(f"⚠️ OpenAI rewrite failed: {e}")
        return text  # fallback

quotes_long["Quote"] = quotes_long["Quote"].apply(rewrite_quote)

# === 8. Select up to 5 quotes per workshop (no duplicates, no padding) ===
def shorten(text, max_len=MAX_LEN):
    if len(text) > max_len:
        return text[:max_len].rsplit(" ", 1)[0] + "..."
    return text

quotes_long["Score"] = quotes_long["Quote"].str.len()

spotlight_all = []
for workshop, group in quotes_long.groupby("Workshop Title"):
    selected = []
    # Try to grab 1 per category
    for cat in QUOTE_COLUMNS:
        subset = group[group["QuoteType"] == cat]
        if not subset.empty:
            selected.append(subset.sort_values("Score", ascending=False).iloc[0])
    # Fill up to 5 max
    if len(selected) < 5:
        extra = group.sort_values("Score", ascending=False)
        for _, row in extra.iterrows():
            if len(selected) >= 5:
                break
            if row["Quote"] not in [s["Quote"] for s in selected]:
                selected.append(row)
    # Limit to actual available quotes (don’t pad/duplicate)
    workshop_quotes = pd.DataFrame(selected).drop_duplicates(subset=["Quote"]).reset_index(drop=True)
    workshop_quotes["Order"] = workshop_quotes.index + 1
    workshop_quotes["Quote"] = workshop_quotes["Quote"].apply(lambda x: shorten(x, MAX_LEN))
    spotlight_all.append(workshop_quotes)

spotlight = pd.concat(spotlight_all, ignore_index=True)

# === 9. Export ===
spotlight = spotlight[["Workshop Title", "Order", "Quote", "Organization", "Economy"]]
spotlight.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")

print("✅ Exported polished Spotlight Quotes (APEC only, ≤5 each) to", OUTPUT_FILE)
print(spotlight.head(15))
