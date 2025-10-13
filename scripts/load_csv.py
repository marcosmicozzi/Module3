import pandas as pd
import psycopg
from datetime import datetime
from dotenv import load_dotenv
import os

# Load environment
load_dotenv()
dbconn = os.getenv("DB_CONN")

# ---------------- BTC PRICE DATA ---------------- #
def load_btc_data():
    print("📊 Loading btc_data.csv...")
    df_btc = pd.read_csv("data/btc_data.csv")

    conn = psycopg.connect(dbconn)
    cur = conn.cursor()

    for _, row in df_btc.iterrows():
        cur.execute("""
            INSERT INTO btc_data (date, open, close, high, low, volume)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (date) DO NOTHING;
        """, (
            datetime.strptime(str(row["date"]), "%Y-%m-%d"),
            float(row["open"]),
            float(row["close"]),
            float(row["high"]),
            float(row["low"]),
            float(row["volume"])
        ))

    conn.commit()
    cur.close()
    conn.close()
    print("✅ btc_data table populated successfully.\n")


# ---------------- BTC ARTICLES ---------------- #
def load_btc_articles():
    print("📰 Loading sentiment_data.csv...")
    df_articles = pd.read_csv("data/sentiment_data.csv")
    df_articles["date"] = pd.to_datetime(df_articles["date"], errors="coerce").dt.date

    conn = psycopg.connect(dbconn)
    cur = conn.cursor()

    # Create a unique article_id if it doesn’t exist
    if "article_id" not in df_articles.columns:
        df_articles["article_id"] = ["BTC" + str(i+1).zfill(4) for i in range(len(df_articles))]

    for _, row in df_articles.iterrows():
        cur.execute("""
            INSERT INTO btc_articles (article_id, title, author, date)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (article_id) DO NOTHING;
        """, (
            row["article_id"],
            row.get("title"),
            row.get("author"),
            row.get("date")
        ))

    conn.commit()
    cur.close()
    conn.close()
    print("✅ btc_articles table populated successfully.\n")


# ---------------- SENTIMENT TABLE ---------------- #
def load_btc_sentiment():
    print("🧠 Loading sentiment scores (if available)...")
    df_sentiment = pd.read_csv("data/sentiment_data.csv")

    df_sentiment["date"] = pd.to_datetime(df_sentiment["date"], errors="coerce").dt.date


    conn = psycopg.connect(dbconn)
    cur = conn.cursor()

    for _, row in df_sentiment.iterrows():
        cur.execute("""
            INSERT INTO btc_sentiment (article_id, sentiment_score, date)
            VALUES (%s, %s, %s)
            ON CONFLICT (article_id) DO NOTHING;
        """, (
            row["article_id"],
            row.get("sentiment_score"),
            row.get("date")
        ))

    conn.commit()
    cur.close()
    conn.close()
    print("✅ btc_sentiment table populated successfully.\n")


# ---------------- RUN ALL ---------------- #
if __name__ == "__main__":
    load_btc_data()
    load_btc_articles()
    load_btc_sentiment()
    print("🚀 All CSV data successfully loaded into Supabase.")