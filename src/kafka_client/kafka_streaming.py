from confluent_kafka import Producer
import json
import praw
import re
import os
from dotenv import load_dotenv


load_dotenv(dotenv_path=r'C:\Users\hp\Downloads\DE-pipeline-project\.env')


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


def fetch_reddit_posts(reddit_client, subreddit_name, limit=10):
    """
    Fetch posts from a subreddit and return them as a list of dictionaries.
    """
    posts = []
    for submission in reddit_client.subreddit(subreddit_name).hot(limit=limit):
        source_url = extract_source_url(submission.selftext) or submission.url
        post_data = {
            "id": submission.id,
            "title": submission.title,
            "author": str(submission.author),
            "subreddit": submission.subreddit.display_name,
            "upvotes": submission.score,
            "downvotes": 0,  # Reddit API doesn't provide downvotes directly
            "num_comments": submission.num_comments,
            "created_utc": submission.created_utc,
            "url": submission.url,
            "source_url": source_url,
            "text": submission.selftext
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

    KAFKA_BOOTSTRAP_SERVERS = 'localhost:9094'
    KAFKA_TOPIC = 'reddit_posts'

    SUBREDDIT_NAME = "TodayILearned"
    FETCH_LIMIT = 1000

    reddit_client = initialize_reddit_client(CLIENT_ID, CLIENT_SECRET, USER_AGENT)

    kafka_producer = initialize_kafka_producer(KAFKA_BOOTSTRAP_SERVERS)

    posts = fetch_reddit_posts(reddit_client, SUBREDDIT_NAME, FETCH_LIMIT)

    send_to_kafka(kafka_producer, KAFKA_TOPIC, posts)

    print(f"Sent {len(posts)} posts to Kafka topic '{KAFKA_TOPIC}'.")

if __name__ == "__main__":
    stream()