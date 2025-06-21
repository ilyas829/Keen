import requests
import logging

logger = logging.getLogger(__name__)

def fetch_news(api_key, query, limit):
    """Fetch news through API"""
    url = ""
    params = {
        "query":query,
        "apiKey":api_key,
        "pageSize": limit,
        "language": "en"
    }
    try:
        response = requests.get(url, params= params)
        response.raise_for_status()
        articles = response.json().get("articles",[])
        logger.info(f"Fetched {len(articles)} articles.")
        return articles
    except requests.RequestException as e:
        logger.error(f"Failed to fetch news :{str(e)}")
        return []