#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Feedback Analysis Pipeline (Optimistic Thresholds)
--------------------------------------------------
1. Cleans feedback text
2. Word + phrase frequency analysis
3. Sentiment analysis using Hugging Face CardiffNLP RoBERTa (3 classes)
   - Optimistic thresholds:
     * POSITIVE if score >= 0.45
     * NEGATIVE if score >= 0.25
     * else Neutral
4. Structured question mapping (from `Question`/`Response` columns in long file)
5. Exports:
   - word_frequency.csv
   - word_frequency_detailed.csv
   - sentiment_summary.csv
   - sentiment_by_question.csv
   - top_phrases.csv
"""

import pandas as pd
import re, string
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

# Load Hugging Face 3-class sentiment pipeline
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
    """Sentiment using CardiffNLP 3-class model with optimistic thresholds."""
    if not text or text.strip() == "":
        return "Neutral"

    results = sentiment_pipeline(text[:512], top_k=None)
    if isinstance(results, list) and isinstance(results[0], list):
        results = results[0]

    # Find scores
    scores = {}
    for r in results:
        label = str(r["label"]).upper()
        if label in ["LABEL_0", "NEGATIVE"]:
            scores["Negative"] = r["score"]
        elif label in ["LABEL_1", "NEUTRAL"]:
            scores["Neutral"] = r["score"]
        elif label in ["LABEL_2", "POSITIVE"]:
            scores["Positive"] = r["score"]

    if not scores:
        return "Neutral"

    # Apply optimistic thresholds
    if "Positive" in scores and scores["Positive"] >= 0.45:
        return "Positive"
    if "Negative" in scores and scores["Negative"] >= 0.25:
        return "Negative"
    return "Neutral"

def map_structured_sentiment(question, response):
    """Maps Likert-style responses into Positive/Neutral/Negative."""
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
        },
        "Challenges": {
            "Yes: It directly addressed key challenges in a meaningful way": "Positive",
            "Somewhat: It addressed some relevant challenges, but not comprehensively": "Neutral",
            "No: It did not substantially address the main challenges we are facing": "Negative"
        }
    }
    return mappings.get(question, {}).get(response, "Neutral")

# ----------------------------
# Main Preprocess
# ----------------------------
def preprocess_feedback():
    df = pd.read_csv(INPUT_FILE)
    df.columns = df.columns.str.strip().str.lower()

    records = []
    phrase_records = []
    sentiment_records = []
    by_question_records = []

    # --- Structured sentiment from Question/Response ---
    if "question" in df.columns and "response" in df.columns:
        for _, row in df.iterrows():
            q = str(row["question"]).strip().lower()
            r = str(row["response"]).strip()
            if not q or not r:
                continue

            if "relevance" in q:
                qtype = "Relevance"
            elif "knowledge" in q:
                qtype = "Knowledge"
            elif "application intent" in q:
                qtype = "Application"
            elif "sharing intent" in q:
                qtype = "Sharing"
            elif "challenge" in q:
                qtype = "Challenges"
            else:
                continue

            sent = map_structured_sentiment(qtype, r)
            sentiment_records.append(sent)
            by_question_records.append((qtype, sent))

    # --- Open-text fields ---
    text_fields = {
        "sharing examples": "Sharing (Open Text)",
        "application examples": "Applications (Open Text)"
    }

    for col, source in text_fields.items():
        if col in df.columns:
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
                        phrase = " ".join(gram)
                        phrase_records.append((phrase, source))

    # --- Word frequency outputs ---
    if records:
        df_words = pd.DataFrame(records, columns=["Word", "Source"])
        df_counts = df_words.groupby(["Word", "Source"]).size().reset_index(name="Frequency")
        df_total = df_words.groupby("Word").size().reset_index(name="TotalFrequency")
        df_detailed = pd.merge(df_counts, df_total, on="Word").sort_values(by="TotalFrequency", ascending=False)
        df_total_only = df_total.sort_values(by="TotalFrequency", ascending=False)

        df_total_only.to_csv(OUTPUT_FILE_TOTAL, index=False)
        df_detailed.to_csv(OUTPUT_FILE_DETAILED, index=False)

    # --- Phrase frequency output ---
    if phrase_records:
        df_phrases = pd.DataFrame(phrase_records, columns=["Phrase", "Source"])
        df_phrases_count = df_phrases.groupby("Phrase").size().reset_index(name="Frequency")
        df_phrases_top = df_phrases_count.sort_values(by="Frequency", ascending=False).head(20)
        df_phrases_top.to_csv(OUTPUT_FILE_PHRASES, index=False)

    # --- Sentiment summary ---
    if sentiment_records:
        df_sentiment = pd.Series(sentiment_records).value_counts().reset_index()
        df_sentiment.columns = ["Sentiment", "Count"]
        df_sentiment.to_csv(OUTPUT_FILE_SENTIMENT, index=False)

    # --- Sentiment by question ---
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

    print("✅ Preprocessing complete")
    print(
        f"Saved {OUTPUT_FILE_TOTAL}, {OUTPUT_FILE_DETAILED}, {OUTPUT_FILE_PHRASES}, "
        f"{OUTPUT_FILE_SENTIMENT}, {OUTPUT_FILE_SENTIMENT_BYQ}"
    )

if __name__ == "__main__":
    preprocess_feedback()
