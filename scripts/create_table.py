import psycopg2
import os
from dotenv import load_dotenv
from configs.db_config import DB_NAME, DB_USER, DB_HOST,PORT
from pathlib import Path
import logging

load_dotenv()
LOG_FILE = Path(__file__).resolve().parent / "table.log"
logging.basicConfig(
    filename=str(LOG_FILE),
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

PG_PASSWORD = os.getenv("PG_PASSWORD")

conn = psycopg2.connect(
    dbname=DB_NAME,
    user=DB_USER,
    password=PG_PASSWORD,
    host=DB_HOST,
    port=PORT,
)
conn.autocommit = True

cursor = conn.cursor()

create_table_query = """
CREATE TABLE IF NOT EXISTS reddit_posts (
    id TEXT PRIMARY KEY,
    text TEXT NOT NULL,
    author TEXT,
    subreddit TEXT,
    upvotes INT,
    downvotes INT DEFAULT 0,
    num_comments INT,
    created_utc TIMESTAMP,
    url TEXT,
    source_url TEXT
);
"""

cursor.execute(create_table_query)
logging.info("Table 'reddit_posts' created successfully!")

cursor.close()
conn.close()