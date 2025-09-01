import pandas as pd
import re, string
from collections import Counter

# ----------------------------
# CONFIG
# ----------------------------
INPUT_FILE = "Feedback_Form_Data_Long.csv"   # your raw export file
OUTPUT_FILE = "word_frequency.csv"           # output for Tableau

# Columns to use and their "Source" labels
TEXT_FIELDS = {
    "Suggested Improvements": "Improvements",
    "Sharing Examples": "Sharing",
    "Potential Barriers": "Barriers",
    "Application Examples": "Applications"
}

STOPWORDS = set([
    "the","was","very","and","me","to","i","a","but","more","new","them","my",
    "in","on","of","for","it","this","that","is","with","at","an","be","we",
    "by","or","as","our","are","will","can","from","have","not","has","had"
])

def clean_text(text):
    if pd.isna(text):
        return ""
    text = text.lower()
    text = re.sub(f"[{re.escape(string.punctuation)}]", "", text)
    return text

# ----------------------------
# MAIN
# ----------------------------
def preprocess_feedback():
    df = pd.read_csv(INPUT_FILE)

    records = []
    for col, source in TEXT_FIELDS.items():
        if col in df.columns:
            for text in df[col].dropna():
                cleaned = clean_text(text)
                for word in cleaned.split():
                    if word not in STOPWORDS and len(word) > 2:
                        records.append((word, source))

    df_words = pd.DataFrame(records, columns=["Word", "Source"])

    # Count per source
    df_counts = df_words.groupby(["Word","Source"]).size().reset_index(name="Frequency")

    # Total counts across all sources
    df_total = df_words.groupby("Word").size().reset_index(name="TotalFrequency")

    # Merge
    df_final = pd.merge(df_counts, df_total, on="Word")

    # Sort
    df_final = df_final.sort_values(by="TotalFrequenc_
