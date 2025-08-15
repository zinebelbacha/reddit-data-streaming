from pathlib import Path
import os
BASE_DIR = Path(__file__).resolve().parents[2]
LOG_DIR = BASE_DIR / "logs"
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = LOG_DIR / "general.log" 

PATH_LAST_PROCESSED= BASE_DIR / "data" / "last_processed.json"

KAFKA_BOOTSTRAP_SERVERS = 'kafka:9092'
KAFKA_TOPIC = 'reddit_posts'

SUBREDDIT_NAME = "TodayILearned"
FETCH_LIMIT = 1000



