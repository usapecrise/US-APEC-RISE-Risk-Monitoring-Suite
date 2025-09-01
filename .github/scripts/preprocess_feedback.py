import pandas as pd
import re, string
from nltk.stem import WordNetLemmatizer

# ----------------------------
# CONFIG
# ----------------------------
INPUT_FILE = "Feedback_Form_Data_Long.csv"   # file already exported earlier in workflow
OUTPUT_FILE = "word_frequency.csv"           # output for Tableau

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

lemmatizer = WordNetLemmatizer()

def clean_text(text):
    if pd.isna(text):
        return ""
    text = text.lower()
    text = re.sub(f"[{re.escape(string.punctuation)}]", "", text)
    return text

def normalize_word(word: str) -> str:
    # Lemmatize verbs (so "sharing" → "share") and nouns ("applications" → "application")
    word = lemmatizer.lemmatize(word, pos="v")
    word = lemmatizer.lemmatize(word, pos="n")
    return word

def preprocess_feedback():
    # Load exported feedback
    df = pd.read_csv(INPUT_FILE)

    records = []
    for col, source in TEXT_FIELDS.items():
        if col in df.columns:
            for text in df[col].dropna():
                cleaned = clean_text(text)
                for word in cleaned.split():
                    if word not in STOPWORDS and len(word) > 2:
                        word = normalize_word(word)   # 👈 normalize before adding
                        records.append((word, source))

    df_words = pd.DataFrame(records, columns=["Word", "Source"])

    # Count per source
    df_counts = df_words.groupby(["Word","Source"]).size().reset_index(name="Frequency")

    # Totals across all sources
    df_total = df_words.groupby("Word").size().reset_index(name="TotalFrequency")

    # Merge
    df_final = pd.merge(df_counts, df_total, on="Word")

    # Sort
    df_final = df_final.sort_values(by="TotalFrequency", ascending=False)

    # Save to repo root (so GitHub Action can commit it like other CSVs)
    df_final.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Saved word frequencies to {OUTPUT_FILE}")

if __name__ == "__main__":
    preprocess_feedback()
