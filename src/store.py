import sqlite3
import pandas as pd
import logging
from urllib.parse import urlparse, urlunparse

logger = logging.getLogger(__name__)

def normalize_url(url):
    """Normalize URL to prevent duplicates (e.g., remove query params, trailing slashes)."""
    try:
        parsed = urlparse(url)
        # Keep only scheme, netloc, and path; remove query and fragment
        normalized = urlunparse((parsed.scheme, parsed.netloc, parsed.path.rstrip('/'), '', '', ''))
        return normalized.lower()
    except Exception as e:
        logger.warning(f"Failed to normalize URL {url}: {str(e)}")
        return url

def save_to_db(df):
    """Save processed data to SQLite database, appending new records and avoiding duplicates."""
    try:
        # Create a copy of the DataFrame
        df = df.copy()
        
        # Sanitize column names for SQLite
        df.columns = [col.replace(" ", "_").lower() for col in df.columns]
        
        # Convert complex data to strings
        if "source_name" in df.columns:
            df["source_name"] = df["source_name"].astype(str)
        if "publishedat" in df.columns:
            df["publishedat"] = df["publishedat"].astype(str)
        
        # Normalize URLs for deduplication
        df["url"] = df["url"].apply(normalize_url)
        
        # Connect to SQLite
        conn = sqlite3.connect("D:\\Keen\\news_data.db")
        
        # Load existing URLs to avoid duplicates
        try:
            existing_urls = pd.read_sql_query("SELECT url FROM articles", conn)["url"].tolist()
            existing_urls = [normalize_url(url) for url in existing_urls]
        except pd.io.sql.DatabaseError:
            existing_urls = []  # Table doesn't exist yet
        
        # Filter out duplicate records
        if existing_urls:
            initial_count = len(df)
            df = df[~df["url"].isin(existing_urls)]
            logger.info(f"Filtered out {initial_count - len(df)} duplicate records.")
        
        if df.empty:
            logger.info("No new records to save.")
            return
        
        # Append new records to the articles table
        df.to_sql("articles", conn, if_exists="append", index=False, dtype={"publishedat": "TEXT"})
        
        logger.info(f"Saved {len(df)} new records to SQLite database.")
    except Exception as e:
        logger.error(f"Failed to save data: {str(e)}")
        raise
    finally:
        conn.commit()
        conn.close()