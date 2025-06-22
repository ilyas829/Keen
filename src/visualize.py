import streamlit as st
import sqlite3
import pandas as pd
import plotly.express as px
import logging

logger = logging.getLogger(__name__)

def run_dashboard():
    """Launch Streamlit dashboard to visualize news data."""
    st.title("Automated News Analysis Dashboard")
    
    try:
        # Load data from SQLite
        conn = sqlite3.connect("news_data.db")
        df = pd.read_sql_query("SELECT * FROM articles order by publishedat", conn)
        conn.close()
        
        if df.empty:
            st.warning("No data available in the database.")
            return
        
        # Display raw data
        st.subheader("News Articles")
        st.dataframe(df[["title", "source_name","description", "publishedat", "sentiment", "confidence"]])
        
        # Sentiment distribution
        st.subheader("Sentiment Distribution")
        fig = px.histogram(df, x="sentiment", color="sentiment", title="Sentiment Analysis Results")
        st.plotly_chart(fig)
        
        # Sentiment by source
        st.subheader("Sentiment by News Source")
        fig = px.bar(df, x="source_name", y="confidence", color="sentiment", title="Sentiment Confidence by Source")
        st.plotly_chart(fig)
        
    except sqlite3.OperationalError as e:
        logger.error(f"Database error: {str(e)}")
        st.error("Error: No data table found. Please run the pipeline to populate the database.")
    except Exception as e:
        logger.error(f"Dashboard failed: {str(e)}")
        st.error(f"Dashboard failed: {str(e)}")

if __name__ == "__main__":
    run_dashboard()