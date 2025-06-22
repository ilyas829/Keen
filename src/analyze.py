from transformers import pipeline
import pandas as pd
import logging

logger = logging.getLogger(__name__)

def analyze_sentiment(df):
    """Perform sentiment analysis on news articles."""
    try:
        # Initialize Hugging Face sentiment analysis pipeline
        classifier = pipeline("sentiment-analysis", model="distilbert-base-uncased-finetuned-sst-2-english")
        
        # Combine title and description for analysis
        texts = (df["title"] + " " + df["description"]).tolist()
        
        # Analyze sentiment
        results = classifier(texts)
        
        # Add sentiment scores to DataFrame
        df["sentiment"] = [r["label"] for r in results]
        df["confidence"] = [r["score"] for r in results]
        
        logger.info("Sentiment analysis completed.")
        return df
    except Exception as e:
        logger.error(f"Sentiment analysis failed: {str(e)}")
        return df