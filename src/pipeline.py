import logging
import sqlite3
import yaml
from ingest import fetch_news
from process import clean_data
from analyze import analyze_sentiment
from store import save_to_db
from visualize import run_dashboard

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_config():
    """Load configuration from YAML file."""
    try:
        with open("D:\\Keen\\src\\config.yaml", "r") as file:
            config = yaml.safe_load(file)
        return config["newsapi"]["api_key"]
    except Exception as e:
        logger.error(f"Failed to load config: {str(e)}")
        raise

def run_pipeline(api_key, query='technology', limit=10):
    """Run the automated news data pipeline."""
    try:
        # Step 1: Fetch news data
        logger.info("Fetching news articles...")
        raw_data = fetch_news(api_key, query, limit)
        if not raw_data:
            logger.error("No data fetched. Exiting pipeline.")
            return

        # Step 2: Clean and process data
        logger.info("Processing data...")
        processed_data = clean_data(raw_data)
        if processed_data.empty:
            logger.error("No data after processing. Exiting pipeline.")
            return

        # Step 3: Perform sentiment analysis
        logger.info("Analyzing sentiment...")
        analyzed_data = analyze_sentiment(processed_data)
        if analyzed_data.empty:
            logger.error("No data after analysis. Exiting pipeline.")
            return

        # Step 4: Store results in SQLite
        logger.info("Saving to database...")
        save_to_db(analyzed_data)
        
        # Verify table exists
        conn = sqlite3.connect("D:\\Keen\\news_data.db")
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='articles'")
        if not cursor.fetchone():
            logger.error("Table 'articles' not created. Exiting pipeline.")
            return
        conn.close()

        # Step 5: Launch Streamlit dashboard
        logger.info("Starting visualization dashboard...")
        run_dashboard()

    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise

if __name__ == "__main__":
    API_KEY = load_config()
    run_pipeline(API_KEY)