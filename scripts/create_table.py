import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

DB_NAME = "postgres"
DB_USER = "postgres"
PG_PASSWORD = os.getenv("PG_PASSWORD")
DB_HOST = "localhost"

conn = psycopg2.connect(
    dbname=DB_NAME,
    user=DB_USER,
    password=PG_PASSWORD,
    host=DB_HOST,
)
conn.autocommit = True

cursor = conn.cursor()

create_table_query = """
CREATE TABLE IF NOT EXISTS reddit_posts (
    id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    author TEXT,
    subreddit TEXT,
    upvotes INT,
    downvotes INT DEFAULT 0,
    num_comments INT,
    created_utc TIMESTAMP,
    url TEXT,
    source_url TEXT,
    text TEXT
);
"""

cursor.execute(create_table_query)

print("Table 'reddit_posts' created successfully!")

cursor.close()
conn.close()