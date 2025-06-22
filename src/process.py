import pandas as pd
import logging

logger = logging.getLogger(__name__)

def clean_data(raw_data):
    """Clean and structure raw news data."""
    try:
        # Convert to DataFrame
        df = pd.DataFrame(raw_data)
        
        # Select relevant columns
        df = df[["title", "description", "publishedAt", "source", "url"]]
        
        # Handle missing values
        df.fillna({"title": "", "description": "", "source": {"name": "Unknown"}}, inplace=True)
        
        # Extract source name
        df["source_name"] = df["source"].apply(lambda x: x.get("name", "Unknown") if isinstance(x, dict) else str(x))
        df.drop(columns=["source"], inplace=True)  # Drop original source column
        
        # Convert publishedAt to string to avoid SQLite issues
        df["publishedAt"] = pd.to_datetime(df["publishedAt"]).astype(str)
        
        logger.info("Data cleaning completed.")
        return df
    except Exception as e:
        logger.error(f"Data cleaning failed: {str(e)}")
        return pd.DataFrame()