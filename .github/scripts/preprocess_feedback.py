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
OUTPUT_FILE_SENTIMENT_BYQ = "sentiment_by_question.csv"  # NEW

TEXT_FIELDS = {
    "Suggested Improvements": "Improvements",
    "Sharing Examples": "Sharing",
    "Potential Barriers": "Barriers",
    "Application Examples": "Applications",
    "Do you have any suggestions to improve future workshops (e.g., content, format, duration, facilitation, or follow-up)?": "Future Workshop Suggestions"
}

STRUCTURED_FIELDS = {
    "To what extent was this training relevant to your field of work?": "relevance",
    "To what extent did this training increase your knowledge in the topic area?": "knowledge",
    "Do you intend to directly apply workshop outcomes in your work?": "application",
    "Did the U.S. technical assistance help address real-world challenges in your economy or sector?": "challenges",
    "Do you plan to share what you learned with others in your organization or professional network?": "sharing"
}

STOPWORDS = set([
    "the","was","very","and","me","to","i","a","but","more","new","them","my",
    "in","on","of","for","it","this","that","is","with","at","an","be","we",
    "by","or","as","our","are","will","can","from","have","not","has","had"
])

lemmatizer = WordNetLemmatizer()
analyzer = SentimentIntensityAnalyzer()

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
    """Sentiment for open-text responses"""
    if not text or text.strip() == "":
        return "Neutral"
    score = analyzer.polarity_scores(text)["compound"]
    if score > 0.05:
        return "Positive"
    elif score < -0.05:
        return "Negative"
    else:
        return "Neutral"

def map_structured_sentiment(question, response):
    mappings = {
        "relevance": {
            "Not at all relevant": "Negative",
            "Slightly relevant": "Mild Negative",
            "Somewhat relevant": "Neutral",
            "Considerably relevant": "Positive",
            "Greatly relevant": "Strong Positive"
        },
        "knowledge": {
            "No increase at all": "Negative",
            "Slightly increased": "Mild Negative",
            "Somewhat increased": "Neutral",
            "Considerably increased": "Positive",
            "Greatly increased": "Strong Positive"
        },
        "application": {
            "Yes: I expect to incorporate them routinely in my day-to-day tasks": "Positive",
            "Somewhat: I may apply them occasionally when circumstances warrant": "Neutral",
            "No: I do not foresee any practical use in my current role": "Negative"
        },
        "challenges": {
            "Yes: It directly addressed key challenges in a meaningful way": "Positive",
            "Somewhat: It addressed some relevant challenges, but not comprehensively": "Neutral",
            "No: It did not substantially address the main challenges we are facing": "Negative"
        },
        "sharing": {
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
    # Load exported feedback
    df = pd.read_csv(INPUT_FILE)

    records = []
    sentiment_records = []
    by_question_records = []

    # ── Structured sentiment mapping ─────────────────────
    for col, qtype in STRUCTURED_FIELDS.items():
        if col in df.columns:
            for resp in df[col].dropna():
                sent = map_structured_sentiment(qtype, resp)
                sentiment_records.append(sent)
                by_question_records.append((qtype.capitalize(), sent))

    # ── Open-text analysis ───────────────────────────────
    for col, source in TEXT_FIELDS.items():
        if col in df.columns:
            for text in df[col].dropna():
                cleaned = clean_text(text)

                # Sentiment classification (per response, not per word)
                sentiment = get_sentiment(text)
                sentiment_records.append(sentiment)
                by_question_records.append((source, sentiment))

                # Tokenize into words
                for word in cleaned.split():
                    if word not in STOPWORDS and len(word) > 2:
                        word = normalize_word(word)
                        records.append((word, source))

    # ── Word frequency outputs ───────────────────────────
    df_words = pd.DataFrame(records, columns=["Word", "Source"])
    if not df_words.empty:
        df_counts = df_words.groupby(["Word", "Source"]).size().reset_index(name="Frequency")
        df_total = df_words.groupby("Word").size().reset_index(name="TotalFrequency")
        df_detailed = pd.merge(df_counts, df_total, on="Word").sort_values(by="TotalFrequency", ascending=False)
        df_total_only = df_total.sort_values(by="TotalFrequency", ascending=False)

        df_total_only.to_csv(OUTPUT_FILE_TOTAL, index=False)
        df_detailed.to_csv(OUTPUT_FILE_DETAILED, index=False)
        print(f"✅ Saved {OUTPUT_FILE_TOTAL} (collapsed by Word)")
        print(f"✅ Saved {OUTPUT_FILE_DETAILED} (detailed with Source breakdown)")

    # ── Sentiment summary outputs ────────────────────────
    df_sentiment = pd.Series(sentiment_records).value_counts(normalize=False).reset_index()
    df_sentiment.columns = ["Sentiment", "Count"]
    df_sentiment.to_csv(OUTPUT_FILE_SENTIMENT, index=False)
    print(f"✅ Saved {OUTPUT_FILE_SENTIMENT} (Positive / Neutral / Negative counts)")

    # NEW: Sentiment by question with percentages
    df_byq = pd.DataFrame(by_question_records, columns=["Question", "Sentiment"])
    df_byq_summary = df_byq.value_counts().reset_index(name="Count")

    # Normalize within each question to get percentages
    df_byq_pct = df_byq.groupby("Question")["Sentiment"].value_counts(normalize=True).mul(100).reset_index(name="Percent")

    # Merge counts + percentages into one file
    df_final = pd.merge(df_byq_summary, df_byq_pct, on=["Question", "Sentiment"])
    df_final.to_csv(OUTPUT_FILE_SENTIMENT_BYQ, index=False)

    print(f"✅ Saved {OUTPUT_FILE_SENTIMENT_BYQ} (sentiment breakdown by question, with % values)")

if __name__ == "__main__":
    preprocess_feedback()
