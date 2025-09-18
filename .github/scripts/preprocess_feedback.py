#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Feedback Analysis Pipeline (Long-Format Version)
------------------------------------------------
1. Reads Feedback_Form_Data_Long.csv with columns: Question, Response
2. Maps structured Likert questions (Relevance, Knowledge, Application, Sharing)
3. Runs Hugging Face sentiment on open-text (Application Examples, Sharing Examples)
4. Skips Barriers + Improvements
5. Exports CSVs with fixed categories so Tableau always sees:
   Relevance, Knowledge, Application, Sharing,
   Applications (Open Text), Sharing (Open Text)
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
OUTPUT_FILE_SENTIMENT = "sentiment_summary.csv"
OUTPUT_FILE_SENTIMENT_BYQ = "sentiment_by_question.csv"
OUTPUT_FILE_TOTAL = "word_frequency.csv"
OUTPUT_FILE_DETAILED = "word_frequency_detailed.csv"
OUTPUT_FILE_PHRASES = "top_phrases.csv"

STOPWORDS = set([
    "the","was","very","and","me","to","i","a","but","more","new","them","my",
    "in","on","of","for","it","this","that","is","with","at","an","be","we",
    "by","or","as","our","are","will","can","from","have","not","has","had"
])

lemmatizer = WordNetLemmatizer()

# Hugging Face sentiment pipeline (3-class)
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
    results = sentiment_pipeline(text[:512], top_k=None)[0]  # list of dicts
    scores = {}
    for r in results:
        label = r["label"].upper()
        if label in ["LABEL_0", "NEGATIVE"]:
            scores["Negative"] = r["score"]
        elif label in ["LABEL_1", "NEUTRAL"]:
            scores["Neutral"] = r["score"]
        elif label in ["LABEL_2", "POSITIVE"]:
            scores["Positive"] = r["score"]
    return max(scores, key=scores.get) if scores else "Neutral"

def map_structured_sentiment(qtype, response):
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
    return mappings.get(qtype, {}).get(response, "Neutral")

# ----------------------------
# Main Preprocess
# ----------------------------
def preprocess_feedback():
    print("DEBUG - Working directory:", os.getcwd())
    df = pd.read_csv(INPUT_FILE)
    print("DEBUG - Loaded rows:", len(df))
    print("DEBUG - Columns:", df.columns.tolist())

    records, phrase_records, sentiment_records, by_question_records = [], [], [], []

    # Map structured questions by matching substrings in Question text
    for _, row in df.iterrows():
        qtext = str(row.get("Question", "")).lower()
        resp = str(row.get("Response", "")).strip()

        if not resp:
            continue

        if "relevant" in qtext:
            sent = map_structured_sentiment("Relevance", resp)
            sentiment_records.append(sent)
            by_question_records.append(("Relevance", sent))
        elif "knowledge" in qtext:
            sent = map_structured_sentiment("Knowledge", resp)
            sentiment_records.append(sent)
            by_question_records.append(("Knowledge", sent))
        elif "apply" in qtext and "example" not in qtext:
            sent = map_structured_sentiment("Application", resp)
            sentiment_records.append(sent)
            by_question_records.append(("Application", sent))
        elif "share" in qtext and "example" not in qtext:
            sent = map_structured_sentiment("Sharing", resp)
            sentiment_records.append(sent)
            by_question_records.append(("Sharing", sent))

        # Open-text analysis
        elif "application example" in qtext:
            cleaned = clean_text(resp)
            words = [normalize_word(w) for w in cleaned.split() if w not in STOPWORDS and len(w) > 2]
            sentiment = get_sentiment(resp)
            sentiment_records.append(sentiment)
            by_question_records.append(("Applications (Open Text)", sentiment))
            for word in words:
                records.append((word, "Applications (Open Text)"))
            for n in [2, 3]:
                for gram in ngrams(words, n):
                    phrase_records.append((" ".join(gram), "Applications (Open Text)"))

        elif "sharing example" in qtext:
            cleaned = clean_text(resp)
            words = [normalize_word(w) for w in cleaned.split() if w not in STOPWORDS and len(w) > 2]
            sentiment = get_sentiment(resp)
            sentiment_records.append(sentiment)
            by_question_records.append(("Sharing (Open Text)", sentiment))
            for word in words:
                records.append((word, "Sharing (Open Text)"))
            for n in [2, 3]:
                for gram in ngrams(words, n):
                    phrase_records.append((" ".join(gram), "Sharing (Open Text)"))

    # Word frequency
    if records:
        df_words = pd.DataFrame(records, columns=["Word", "Source"])
        df_counts = df_words.groupby(["Word", "Source"]).size().reset_index(name="Frequency")
        df_total = df_words.groupby("Word").size().reset_index(name="TotalFrequency")
        df_detailed = pd.merge(df_counts, df_total, on="Word").sort_values(by="TotalFrequency", ascending=False)
        df_total.sort_values(by="TotalFrequency", ascending=False).to_csv(OUTPUT_FILE_TOTAL, index=False)
        df_detailed.to_csv(OUTPUT_FILE_DETAILED, index=False)

    # Phrase frequency
    if phrase_records:
        df_phrases = pd.DataFrame(phrase_records, columns=["Phrase", "Source"])
        df_phrases_count = df_phrases.groupby("Phrase").size().reset_index(name="Frequency")
        df_phrases_count.sort_values(by="Frequency", ascending=False).head(20).to_csv(OUTPUT_FILE_PHRASES, index=False)

    # Sentiment summary
    if sentiment_records:
        df_sentiment = pd.Series(sentiment_records).value_counts().reset_index()
        df_sentiment.columns = ["Sentiment", "Count"]
        df_sentiment.to_csv(OUTPUT_FILE_SENTIMENT, index=False)

    # Sentiment by question (force all 6 categories)
    expected_categories = [
        "Relevance", "Knowledge", "Application", "Sharing",
        "Applications (Open Text)", "Sharing (Open Text)"
    ]
    sentiments = ["Positive", "Neutral", "Negative"]

    if by_question_records:
        df_byq = pd.DataFrame(by_question_records, columns=["Question", "Sentiment"])
        df_byq_summary = df_byq.value_counts().reset_index(name="Count")
        df_byq_pct = (
            df_byq.groupby("Question")["Sentiment"]
            .value_counts(normalize=True)
            .mul(100)
            .reset_index(name="Percent")
        )
        df_final = pd.merge(df_byq_summary, df_byq_pct, on=["Question", "Sentiment"], how="outer")

        all_combos = pd.MultiIndex.from_product([expected_categories, sentiments], names=["Question","Sentiment"])
        df_final = df_final.set_index(["Question","Sentiment"]).reindex(all_combos, fill_value=0).reset_index()

        df_final.to_csv(OUTPUT_FILE_SENTIMENT_BYQ, index=False)

    print("✅ Preprocessing complete")

if __name__ == "__main__":
    preprocess_feedback()
