import pandas as pd
import re, string
from nltk.stem import WordNetLemmatizer
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# ----------------------------
# CONFIG
# ----------------------------
INPUT_FILE = "Feedback_Form_Data_Long.csv"   
OUTPUT_FILE_TOTAL = "word_frequency.csv"     
OUTPUT_FILE_DETAILED = "word_frequency_detailed.csv"  
OUTPUT_FILE_SENTIMENT = "sentiment_summary.csv"  

TEXT_FIELDS = {
    "Suggested Improvements": "Improvements",
    "Sharing Examples": "Sharing",
    "Potential Barriers": "Barriers",
    "Application Examples": "Applications",
    "Do you have any suggestions to improve future workshops (e.g., content, format, duration, facilitation, or follow-up)?": "Future Workshop Suggestions"
}

STOPWORDS = set([
    "the","was","very","and","me","to","i","a","but","more","new","them","my",
    "in","on","of","for","it","this","that","is","with","at","an","be","we",
    "by","or","as","our","are","will","can","from","have","not","has","had"
])

lemmatizer = WordNetLemmatizer()
analyzer = SentimentIntensityAnalyzer()

def clean_text(text):
    if pd.isna(text):
        return ""
    text = text.lower()
    text = re.sub(f"[{re.escape(string.punctuation)}]", "", text)
    return text

def normalize_word(word: str) -> str:
    word = lemmatizer.lemmatize(word, pos="v")
    word = lemmatizer.lemmatize(word, pos="n")
    return word

def get_sentiment(text: str) -> str:
    if not text or text.strip() == "":
        return "Neutral"
    score = analyzer.polarity_scores(text)["compound"]
    if score > 0.05:
        return "Positive"
    elif score < -0.05:
        return "Negative"
    else:
        return "Neutral"

def preprocess_feedback():
    # Load exported feedback
    df = pd.read_csv(INPUT_FILE)

    records = []
    sentiment_records = []

    for col, source in TEXT_FIELDS.items():
        if col in df.columns:
            for text in df[col].dropna():
                cleaned = clean_text(text)

                # Sentiment classification (per response, not per word)
                sentiment = get_sentiment(text)
                sentiment_records.append(sentiment)

                # Tokenize into words
                for word in cleaned.split():
                    if word not in STOPWORDS and len(word) > 2:
                        word = normalize_word(word)
                        records.append((word, source))

    # ── Word frequency outputs ───────────────────────────
    df_words = pd.DataFrame(records, columns=["Word", "Source"])
    df_counts = df_words.groupby(["Word", "Source"]).size().reset_index(name="Frequency")
    df_total = df_words.groupby("Word").size().reset_index(name="TotalFrequency")
    df_detailed = pd.merge(df_counts, df_total, on="Word").sort_values(by="TotalFrequency", ascending=False)
    df_total_only = df_total.sort_values(by="TotalFrequency", ascending=False)

    # Save word frequency files
    df_total_only.to_csv(OUTPUT_FILE_TOTAL, index=False)
    df_detailed.to_csv(OUTPUT_FILE_DETAILED, index=False)
    print(f"✅ Saved {OUTPUT_FILE_TOTAL} (collapsed by Word)")
    print(f"✅ Saved {OUTPUT_FILE_DETAILED} (detailed with Source breakdown)")

    # ── Sentiment summary output ─────────────────────────
    df_sentiment = pd.Series(sentiment_records).value_counts().reset_index()
    df_sentiment.columns = ["Sentiment", "Count"]
    df_sentiment.to_csv(OUTPUT_FILE_SENTIMENT, index=False)
    print(f"✅ Saved {OUTPUT_FILE_SENTIMENT} (Positive / Neutral / Negative counts)")

if __name__ == "__main__":
    preprocess_feedback()
