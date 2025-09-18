#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Feedback Analysis Pipeline (with Debugging)
-------------------------------------------
1. Cleans feedback text
2. Word + phrase frequency analysis
3. Sentiment analysis using Hugging Face CardiffNLP RoBERTa (3 classes)
4. Structured + open-text mapping
5. Debug prints for troubleshooting file updates
"""

import pandas as pd
import re, string, os
from nltk.stem import WordNetLemmatizer
from nltk.util import ngrams
from transformers import pipeline

# ----------------------------
# CONFIG
# ----------------------------
INPUT_FILE = "Feedback_Form_Data_Long.csv"
OUTPUT_FILE_TOTAL = "word_frequency.csv"
OUTPUT_FILE_DETAILED = "word_frequency_detailed.csv"
OUTPUT_FILE_SENTIMENT = "sentiment_summary.csv"
OUTPUT_FILE_SENTIMENT_BYQ = "sentiment_by_question.csv"
OUTPUT_FILE_PHRASES = "top_phrases.csv" 

STOPWORDS = set([
    "the","was","very","and","me","to","i","a","but","more","new","them","my",
    "in","on","of","for","it","this","that","is","with","at","an","be","we",
    "by","or","as","our","are","will","can","from","have","not","has","had"
])

lemmatizer = WordNetLemmatizer()

# Load Hugging Face pipeline
sentiment_pipeline = pipeline(
    "sentiment-analysis",
    model="cardiffnlp/twitter-roberta-base-sentiment-latest"
)

# ----------------------------
# Helpers
# ----------------------------
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
    results = sentiment_pipeline(text[:512], top_k=None)[0]
    scores = {}
    for r in results:
        label = r["label"].upper()
        if label in ["LABEL_0", "NEGATIVE"]:
            scores["Negative"] = r["score"]
        elif label in ["LABEL_1", "NEUTRAL"]:
            scores["Neutral"] = r["score"]
        elif label in ["LABEL_2", "POSITIVE"]:
            scores["Positive"] = r["score"]
    if scores:
        return max(scores, key=scores.get)
    return "Neutral"

def map_structured_sentiment(question, response):
    mappings = {
        "Relevance": {
            "Not at all relevant": "Negative",
            "Slightly relevant": "Negative",
            "Somewhat relevant": "Neutral",
            "Considerably relevant": "Positive",
            "Greatly relevant": "Positive"
        },
        "Knowledge": {
            "No increase at all": "Negative",
            "Slightly increased": "Negative",
            "Somewhat increased": "Neutral",
            "Considerably increased": "Positive",
            "Greatly increased": "Positive"
        },
        "Application": {
            "Yes: I expect to incorporate them routinely in my day-to-day tasks": "Positive",
            "Somewhat: I may apply them occasionally when circumstances warrant": "Neutral",
            "No: I do not foresee any practical use in my current role": "Negative"
        },
        "Sharing": {
            "Yes: I intend to actively share with colleagues or my network": "Positive",
            "Somewhat: I may share in appropriate settings if relevant": "Neutral",
            "Not at this time: I do not currently have plans to share": "Negative"
        }
    }
    return mappings.get(question, {}).get(response, "Neutral")

# ----------------------------
# Main Preprocess
# ----------------------------
def preprocess_feedback():
    print("DEBUG - Current working directory:", os.getcwd())
    print("DEBUG - Attempting to read:", INPUT_FILE)

    df = pd.read_csv(INPUT_FILE)
    print("DEBUG - Rows loaded:", len(df))
    print("DEBUG - Raw columns:", df.columns.tolist())

    df.columns = df.columns.str.strip().str.lower().str.replace(r"[?]", "", regex=True)
    print("DEBUG - Normalized columns:", df.columns.tolist())

    STRUCTURED_FIELDS = {
        "relevance to work": "Relevance",
        "knowledge gain": "Knowledge",
        "application intent": "Application",
        "sharing intent": "Sharing"
    }

    TEXT_FIELDS = {
        "application examples": "Applications (Open Text)",
        "sharing examples": "Sharing (Open Text)"
    }

    records, phrase_records, sentiment_records, by_question_records = [], [], [], []

    # Structured
    for col, qtype in STRUCTURED_FIELDS.items():
        if col in df.columns:
            print(f"DEBUG - Processing structured field: {col} → {qtype}")
            for resp in df[col].dropna():
                sent = map_structured_sentiment(qtype, resp)
                sentiment_records.append(sent)
                by_question_records.append((qtype, sent))
        else:
            print(f"DEBUG - MISSING structured field: {col}")

    # Open-text
    for col, source in TEXT_FIELDS.items():
        if col in df.columns:
            print(f"DEBUG - Processing open-text field: {col} → {source}")
            for text in df[col].dropna():
                cleaned = clean_text(text)
                words = [normalize_word(w) for w in cleaned.split() if w not in STOPWORDS and len(w) > 2]
                sentiment = get_sentiment(text)
                sentiment_records.append(sentiment)
                by_question_records.append((source, sentiment))
                for word in words:
                    records.append((word, source))
                for n in [2, 3]:
                    for gram in ngrams(words, n):
                        phrase_records.append((" ".join(gram), source))
        else:
            print(f"DEBUG - MISSING open-text field: {col}")

    # Write outputs with debug
    if records:
        pd.DataFrame(records, columns=["Word", "Source"]).to_csv(OUTPUT_FILE_TOTAL, index=False)
        print(f"DEBUG - Wrote {OUTPUT_FILE_TOTAL}")

    if phrase_records:
        pd.DataFrame(phrase_records, columns=["Phrase", "Source"])\
            .groupby("Phrase").size().reset_index(name="Frequency")\
            .sort_values(by="Frequency", ascending=False).head(20)\
            .to_csv(OUTPUT_FILE_PHRASES, index=False)
        print(f"DEBUG - Wrote {OUTPUT_FILE_PHRASES}")

    if sentiment_records:
        df_sentiment = pd.Series(sentiment_records).value_counts().reset_index()
        df_sentiment.columns = ["Sentiment", "Count"]
        df_sentiment.to_csv(OUTPUT_FILE_SENTIMENT, index=False)
        print(f"DEBUG - Wrote {OUTPUT_FILE_SENTIMENT} with rows:", len(df_sentiment))

    if by_question_records:
        df_byq = pd.DataFrame(by_question_records, columns=["Question", "Sentiment"])
        df_byq_summary = df_byq.value_counts().reset_index(name="Count")
        df_byq_pct = (
            df_byq.groupby("Question")["Sentiment"]
            .value_counts(normalize=True)
            .mul(100)
            .reset_index(name="Percent")
        )
        df_final = pd.merge(df_byq_summary, df_byq_pct, on=["Question", "Sentiment"])
        df_final.to_csv(OUTPUT_FILE_SENTIMENT_BYQ, index=False)
        print(f"DEBUG - Wrote {OUTPUT_FILE_SENTIMENT_BYQ} with rows:", len(df_final))

    print("✅ Preprocessing complete")

if __name__ == "__main__":
    preprocess_feedback()
