import pandas as pd
import re, string
from nltk.stem import WordNetLemmatizer
from nltk.util import ngrams
from flair.models import TextClassifier
from flair.data import Sentence

# ----------------------------
# CONFIG
# ----------------------------
INPUT_FILE = "Feedback_Form_Data_Long.csv"
OUTPUT_FILE_TOTAL = "word_frequency.csv"
OUTPUT_FILE_DETAILED = "word_frequency_detailed.csv"
OUTPUT_FILE_SENTIMENT = "sentiment_summary.csv"
OUTPUT_FILE_SENTIMENT_BYQ = "sentiment_by_question.csv"
OUTPUT_FILE_PHRASES = "top_phrases.csv" 

TEXT_FIELDS = {
    "Suggested Improvements": "Improvements",
    "Sharing Examples": "Sharing",
    "Potential Barriers": "Barriers",
    "Application Examples": "Applications"
}

STRUCTURED_FIELDS = {
    "To what extent was this training relevant to your field of work?": "relevance",
    "To what extent did this training increase your knowledge in the topic area?": "knowledge",
    "Do you intend to directly apply workshop outcomes in your work?": "application",
    "Do you plan to share what you learned with others in your organization or professional network?": "sharing"
}

STOPWORDS = set([
    "the","was","very","and","me","to","i","a","but","more","new","them","my",
    "in","on","of","for","it","this","that","is","with","at","an","be","we",
    "by","or","as","our","are","will","can","from","have","not","has","had"
])

lemmatizer = WordNetLemmatizer()
classifier = TextClassifier.load("sentiment")  # Flair sentiment model

# Sentiment thresholds
POS_THRESHOLD = 0.6
NEG_THRESHOLD = 0.75   # stricter: fewer negatives

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
    """Sentiment for open-text responses using Flair with dual thresholds"""
    if not text or text.strip() == "":
        return "Neutral"

    sentence = Sentence(text)
    classifier.predict(sentence)
    label = sentence.labels[0]

    if label.value == "POSITIVE" and label.score >= POS_THRESHOLD:
        return "Positive"
    elif label.value == "NEGATIVE" and label.score >= NEG_THRESHOLD:
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
    df = pd.read_csv(INPUT_FILE)

    records = []
    phrase_records = []
    sentiment_records = []
    by_question_records = []

    # Structured sentiment mapping
    for col, qtype in STRUCTURED_FIELDS.items():
        if col in df.columns:
            for resp in df[col].dropna():
                sent = map_structured_sentiment(qtype, resp)
                sentiment_records.append(sent)
                by_question_records.append((qtype.capitalize(), sent))

    # Open-text analysis
    for col, source in TEXT_FIELDS.items():
        if col in df.columns:
            for text in df[col].dropna():
                cleaned = clean_text(text)
                words = [normalize_word(w) for w in cleaned.split() if w not in STOPWORDS and len(w) > 2]

                # Sentiment
                sentiment = get_sentiment(text)
                sentiment_records.append(sentiment)
                by_question_records.append((source, sentiment))

                # Word-level frequencies
                for word in words:
                    records.append((word, source))

                # Phrase-level (bigrams/trigrams)
                for n in [2, 3]:
                    for gram in ngrams(words, n):
                        phrase = " ".join(gram)
                        phrase_records.append((phrase, source))

    # Word frequency outputs
    if records:
        df_words = pd.DataFrame(records, columns=["Word", "Source"])
        df_counts = df_words.groupby(["Word", "Source"]).size().reset_index(name="Frequency")
        df_total = df_words.groupby("Word").size().reset_index(name="TotalFrequency")
        df_detailed = pd.merge(df_counts, df_total, on="Word").sort_values(by="TotalFrequency", ascending=False)
        df_total_only = df_total.sort_values(by="TotalFrequency", ascending=False)

        df_total_only.to_csv(OUTPUT_FILE_TOTAL, index=False)
        df_detailed.to_csv(OUTPUT_FILE_DETAILED, index=False)

    # Phrase frequency output
    if phrase_records:
        df_phrases = pd.DataFrame(phrase_records, columns=["Phrase", "Source"])
        df_phrases_count = df_phrases.groupby("Phrase").size().reset_index(name="Frequency")
        df_phrases_top = df_phrases_count.sort_values(by="Frequency", ascending=False).head(20)
        df_phrases_top.to_csv(OUTPUT_FILE_PHRASES, index=False)

    # Sentiment summary
    if sentiment_records:
        df_sentiment = pd.Series(sentiment_records).value_counts().reset_index()
        df_sentiment.columns = ["Sentiment", "Count"]
        df_sentiment.to_csv(OUTPUT_FILE_SENTIMENT, index=False)

    # Sentiment by question
    if by_question_records:
        df_byq = pd.DataFrame(by_question_records, columns=["Question", "Sentiment"])
        df_byq_summary = df_byq.value_counts().reset_index(name="Count")
        df_byq_pct = df_byq.groupby("Question")["Sentiment"].value_counts(normalize=True).mul(100).reset_index(name="Percent")
        df_final = pd.merge(df_byq_summary, df_byq_pct, on=["Question", "Sentiment"])
        df_final.to_csv(OUTPUT_FILE_SENTIMENT_BYQ, index=False)

    print("✅ Preprocessing complete")
    print(f"Saved {OUTPUT_FILE_TOTAL}, {OUTPUT_FILE_DETAILED}, {OUTPUT_FILE_PHRASES}, {OUTPUT_FILE_SENTIMENT}, {OUTPUT_FILE_SENTIMENT_BYQ}")

if __name__ == "__main__":
    preprocess_feedback()

