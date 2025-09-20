#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Feedback Analysis Pipeline (Wide File Only, Flair + Context-Aware Rules)
------------------------------------------------------------------------
- Uses Feedback_Form_Data.csv (wide format: 1 row per participant)
- Open-text fields: context-aware sentiment
  * Application/Sharing → default Positive unless clearly Negative
  * Improvements/Barriers → full Positive/Neutral/Negative
- Structured fields: mapped into Positive/Neutral/Negative
- Flair model for sentiment + thresholds + keyword boosts
- Exports:
  - word_frequency.csv
  - word_frequency_detailed.csv
  - sentiment_summary.csv
  - sentiment_by_question.csv
  - sentiment_scores.csv
  - top_phrases.csv
"""

import pandas as pd
import re, string
from nltk.stem import WordNetLemmatizer
from nltk.util import ngrams
from flair.models import TextClassifier
from flair.data import Sentence

# ----------------------------
# CONFIG
# ----------------------------
INPUT_FILE = "Feedback_Form_Data.csv"
OUTPUT_FILE_TOTAL = "word_frequency.csv"
OUTPUT_FILE_DETAILED = "word_frequency_detailed.csv"
OUTPUT_FILE_SENTIMENT = "sentiment_summary.csv"
OUTPUT_FILE_SENTIMENT_BYQ = "sentiment_by_question.csv"
OUTPUT_FILE_PHRASES = "top_phrases.csv"
OUTPUT_FILE_SCORES = "sentiment_scores.csv"

STOPWORDS = set([
    "the","was","very","and","me","to","i","a","but","more","new","them","my",
    "in","on","of","for","it","this","that","is","with","at","an","be","we",
    "by","or","as","our","are","will","can","from","have","not","has","had"
])

lemmatizer = WordNetLemmatizer()

# Load Flair sentiment model
flair_classifier = TextClassifier.load("sentiment")

# ----------------------------
# Thresholds + keyword boost
# ----------------------------
POS_THRESHOLD = 0.6
NEG_THRESHOLD = 0.3

POSITIVE_HINTS = {
    "useful","helpful","valuable","good","clear","great","effective","relevant",
    "apply","applied","implement","implemented","use","using","utilize","practice",
    "adopt","adopted","incorporate","incorporated","plan","planned","will",
    "benefit","beneficial","support","important","improve","improved","gain","learned",
    "continue","bring","share","sharing","spread","discuss","train","teaching",
    "inform","knowledge","practice","follow","followup","build","strengthen","capacity",
    "apply it"
}

NEGATIVE_HINTS = {
    "confusing","unclear","poor","bad","difficult","waste","irrelevant","unhelpful",
    "not useful","lacking","limited","lack","barrier","constraint",
    "shortage","misalignment","none","nothing","too short","too long","timing","duration","cases",
    "delay","support needed"
}

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

def flair_sentiment(text: str):
    """Base Flair model output with thresholds."""
    sentence = Sentence(text[:512])
    flair_classifier.predict(sentence)
    label = sentence.labels[0]
    raw_label = label.value.upper()   # POSITIVE / NEGATIVE
    score = label.score
    pos, neg = 0.0, 0.0
    if raw_label == "POSITIVE":
        pos = score
    elif raw_label == "NEGATIVE":
        neg = score
    neu = 1 - max(pos, neg)

    if pos >= POS_THRESHOLD:
        sentiment = "Positive"
    elif neg >= NEG_THRESHOLD:
        sentiment = "Negative"
    else:
        sentiment = "Neutral"
    return sentiment, {"Positive": pos, "Negative": neg, "Neutral": neu}

def get_sentiment(text: str, source: str):
    """Context-aware sentiment: bias Application/Sharing Positive."""
    if not text or text.strip() == "":
        return "Neutral", {"Positive": 0.0, "Negative": 0.0, "Neutral": 1.0}

    sentiment, scores = flair_sentiment(text)

    # Keyword boost
    if sentiment == "Neutral":
        tokens = set(clean_text(text).split())
        if tokens & POSITIVE_HINTS:
            sentiment = "Positive"
        elif tokens & NEGATIVE_HINTS:
            sentiment = "Negative"
        for phrase in POSITIVE_HINTS:
            if phrase in text.lower():
                sentiment = "Positive"
        for phrase in NEGATIVE_HINTS:
            if phrase in text.lower():
                sentiment = "Negative"

    # Context rules
    if source in ["Applications (Open Text)", "Sharing (Open Text)"]:
        if sentiment == "Neutral":
            sentiment = "Positive"

    return sentiment, scores

def map_structured_sentiment(question, response):
    """Maps Likert-style responses into sentiment."""
    mappings = {
        "Relevance to Work": {
            "Not at all relevant": "Negative",
            "Slightly relevant": "Neutral",
            "Somewhat relevant": "Neutral",
            "Considerably relevant": "Positive",
            "Greatly relevant": "Positive"
        },
        "Knowledge Gain": {
            "No increase at all": "Negative",
            "Slightly increased": "Neutral",
            "Somewhat increased": "Neutral",
            "Considerably increased": "Positive",
            "Greatly increased": "Positive"
        },
        "Application Intent": {
            "Yes: I expect to incorporate them routinely in my day-to-day tasks": "Positive",
            "Somewhat: I may apply them occasionally when circumstances warrant": "Neutral",
            "No: I do not foresee any practical use in my current role": "Negative"
        },
        "Sharing Intent": {
            "Yes: I intend to actively share with colleagues or my network": "Positive",
            "Somewhat: I may share in appropriate settings if relevant": "Neutral",
            "Not at this time: I do not currently have plans to share": "Negative"
        },
        "Challenges Addressed": {
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
    df.columns = df.columns.str.strip()

    records, phrase_records, sentiment_records, by_question_records, score_records = [], [], [], [], []

    # --- Structured fields ---
    structured_fields = ["Relevance to Work", "Knowledge Gain", "Application Intent", "Sharing Intent", "Challenges Addressed"]
    for _, row in df.iterrows():
        for field in structured_fields:
            if field in df.columns and pd.notna(row[field]):
                sent = map_structured_sentiment(field, row[field])
                sentiment_records.append(sent)
                by_question_records.append((field, sent))

    # --- Open-text fields ---
    open_text_fields = {
        "Sharing Examples": "Sharing (Open Text)",
        "Application Examples": "Applications (Open Text)",
    }

    for col, source in open_text_fields.items():
        if col in df.columns:
            for _, text in df[col].dropna().items():
                cleaned = clean_text(text)
                words = [normalize_word(w) for w in cleaned.split() if w not in STOPWORDS and len(w) > 2]

                sentiment, scores = get_sentiment(text, source)
                sentiment_records.append(sentiment)
                by_question_records.append((source, sentiment))
                score_records.append((source, text, sentiment, scores["Positive"], scores["Negative"], scores["Neutral"]))

                for word in words:
                    records.append((word, source))
                for n in [2, 3]:
                    for gram in ngrams(words, n):
                        phrase_records.append((" ".join(gram), source))

    # --- Word frequency ---
    if records:
        df_words = pd.DataFrame(records, columns=["Word", "Source"])
        df_counts = df_words.groupby(["Word", "Source"]).size().reset_index(name="Frequency")
        df_total = df_words.groupby("Word").size().reset_index(name="TotalFrequency")
        df_detailed = pd.merge(df_counts, df_total, on="Word").sort_values(by="TotalFrequency", ascending=False)
        df_total.sort_values(by="TotalFrequency", ascending=False).to_csv(OUTPUT_FILE_TOTAL, index=False)
        df_detailed.to_csv(OUTPUT_FILE_DETAILED, index=False)

    # --- Phrase frequency ---
    if phrase_records:
        df_phrases = pd.DataFrame(phrase_records, columns=["Phrase", "Source"])
        df_phrases.groupby("Phrase").size().reset_index(name="Frequency").sort_values(
            by="Frequency", ascending=False
        ).head(20).to_csv(OUTPUT_FILE_PHRASES, index=False)

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
        pd.merge(df_byq_summary, df_byq_pct, on=["Question", "Sentiment"]).to_csv(OUTPUT_FILE_SENTIMENT_BYQ, index=False)

    # --- Sentiment scores ---
    if score_records:
        pd.DataFrame(
            score_records, columns=["Source", "Text", "Sentiment", "PosScore", "NegScore", "NeuScore"]
        ).to_csv(OUTPUT_FILE_SCORES, index=False)

    # --- Console summary ---
    print("✅ Preprocessing complete")
    print(f"Saved outputs: {OUTPUT_FILE_TOTAL}, {OUTPUT_FILE_DETAILED}, {OUTPUT_FILE_PHRASES}, "
          f"{OUTPUT_FILE_SENTIMENT}, {OUTPUT_FILE_SENTIMENT_BYQ}, {OUTPUT_FILE_SCORES}")
    print("\n--- Sentiment Summary (quick view) ---")
    print(pd.Series(sentiment_records).value_counts())

if __name__ == "__main__":
    preprocess_feedback()
