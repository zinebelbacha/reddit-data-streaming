from confluent_kafka import Producer
import json
import praw
import re
import os
from dotenv import load_dotenv
from pathlib import Path
from configs.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, SUBREDDIT_NAME, FETCH_LIMIT, PATH_LAST_PROCESSED
import logging

load_dotenv()
LOG_FILE = Path(__file__).resolve().parent / "kafka_streaming.log"
logging.basicConfig(
    filename=str(LOG_FILE),
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


def initialize_reddit_client(client_id, client_secret, user_agent):
    """
    Initialize and return a Reddit API client using praw.
    """
    reddit = praw.Reddit(
        client_id=client_id,
        client_secret=client_secret,
        user_agent=user_agent
    )
    return reddit

        
def update_last_processed_file(posts):
    """
    Updates last_processed.json with the most recent post's timestamp.
    """
    if not posts:
        return
    latest_timestamp = max(post["created_utc"] for post in posts)
    with open(PATH_LAST_PROCESSED, "w") as file:
        json.dump({"last_processed": latest_timestamp}, file)


def get_latest_timestamp():
    """
    Gets the latest processed UTC timestamp from file, or 0 if none exists.
    """
    if not os.path.exists(PATH_LAST_PROCESSED):
        return 0
    with open(PATH_LAST_PROCESSED, "r") as file:
        data = json.load(file)
        return data.get("last_processed", 0)


        

def initialize_kafka_producer(bootstrap_servers):
    """
    Initialize and return a Kafka producer using confluent-kafka.
    """
    producer = Producer({
        'bootstrap.servers': bootstrap_servers
    })
    return producer

def extract_source_url(text):
    """
    Extract the first URL from the post text.
    """
    url_pattern = re.compile(r'https?://[^\s]+')
    match = url_pattern.search(text)
    if match:
        return match.group(0)
    return None


def fetch_reddit_posts(reddit_client, subreddit_name, limit=10, last_processed_ts=0):
    """
    Fetch posts newer than last_processed_ts from a subreddit.
    """
    posts = []
    for submission in reddit_client.subreddit(subreddit_name).hot(limit=limit):
        if submission.created_utc <= last_processed_ts:
            continue  # Skip old posts
        source_url = extract_source_url(submission.selftext) or submission.url
        post_data = {
            "id": submission.id,
            "title": submission.title,
            "author": str(submission.author),
            "subreddit": submission.subreddit.display_name,
            "upvotes": submission.score,
            "downvotes": 0,
            "num_comments": submission.num_comments,
            "created_utc": submission.created_utc,
            #"date": datetime.fromtimestamp(submission.created_utc, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S %Z'),
            "url": f"https://www.reddit.com{submission.permalink}",
            "source_url": source_url
            #"text": submission.selftext
        }
        posts.append(post_data)
    return posts



def send_to_kafka(producer, topic, data):
    """
    Send data to a Kafka topic using confluent-kafka.
    """
    for record in data:
        producer.produce(topic, value=json.dumps(record).encode('utf-8'))
    producer.flush()

def stream():
    CLIENT_ID = os.getenv("CLIENT_ID")
    CLIENT_SECRET = os.getenv("CLIENT_SECRET")
    USER_AGENT = os.getenv("USER_AGENT")

    last_processed_ts = get_latest_timestamp()
    reddit_client = initialize_reddit_client(CLIENT_ID, CLIENT_SECRET, USER_AGENT)
    kafka_producer = initialize_kafka_producer(KAFKA_BOOTSTRAP_SERVERS)

    posts = fetch_reddit_posts(reddit_client, SUBREDDIT_NAME, FETCH_LIMIT, last_processed_ts)
    if posts:
        send_to_kafka(kafka_producer, KAFKA_TOPIC, posts)
        update_last_processed_file(posts)
        logging.info(f"Sent {len(posts)} new posts to Kafka topic '{KAFKA_TOPIC}'.")
    else:
        logging.info("No new posts found.")


if __name__ == "__main__":
    stream()