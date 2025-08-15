from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType
)
from pyspark.sql.functions import from_json, col, from_utc_timestamp
import logging
import os, sys
from dotenv import load_dotenv
from configs.config import LOG_FILE
from configs.db_config import URL, POSTGRES_PROPERTIES
import logging

load_dotenv()
logging.basicConfig(
    filename=str(LOG_FILE),
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
url = URL
properties = POSTGRES_PROPERTIES
required_env_vars = ["PG_PASSWORD"]
missing_vars = [var for var in required_env_vars if not os.getenv(var)]

if missing_vars:
    print(f"ERROR: Missing required environment variables: {', '.join(missing_vars)}", file=sys.stderr)
    sys.exit(1)



# Check required environment variables

def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder.appName("PostgreSQL Connection with PySpark")
        .config(
            "spark.jars.packages",
            "org.postgresql:postgresql:42.5.4,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        )
        .config("spark.driver.extraJavaOptions", "-Djava.net.preferIPv4Stack=true")
        .config("spark.executor.extraJavaOptions", "-Djava.net.preferIPv4Stack=true")
        .getOrCreate()
    )

    logging.info("Spark session created successfully")
    return spark


def create_initial_dataframe(spark_session):
    """
    Reads the streaming data and creates the initial dataframe accordingly.
    """
    try:
        df = (
            spark_session.readStream.format("kafka")
            .option("kafka.bootstrap.servers", "kafka:9092")
            .option("subscribe", "reddit_posts")
            .option("startingOffsets", "earliest")
            .load()
        )
        logging.info("Initial dataframe created successfully")
    except Exception as e:
        logging.warning(f"Initial dataframe couldn't be created due to exception: {e}")
        raise

    return df


def create_final_dataframe(df):
    """
    Processes the initial DataFrame to extract and parse JSON data.
    """
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("title", StringType(), True),
        StructField("author", StringType(), True),
        StructField("subreddit", StringType(), True),
        StructField("upvotes", IntegerType(), True),
        StructField("downvotes", IntegerType(), True),
        StructField("num_comments", IntegerType(), True),
        StructField("created_utc", TimestampType(), True),
        StructField("url", StringType(), True),
        StructField("source_url", StringType(), True),
        StructField("text", StringType(), True),
    ])
    
    df_out = (
        df.selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), schema).alias("data"))
        .select("data.*")
    )
    return df_out


def start_streaming(df_parsed, spark):
    """
    Starts the streaming to table 'reddit_posts' in PostgreSQL.
    """
    existing_data_df = spark.read.jdbc(
       url, "reddit_posts", properties=properties
    )

    unique_column = "id"

    logging.info("Start streaming ...")
    query = df_parsed.writeStream.foreachBatch(
        lambda batch_df, _: (
            batch_df.join(
                existing_data_df, batch_df[unique_column] == existing_data_df[unique_column], "leftanti"
            )
            .write.jdbc(
                url, "reddit_posts", "append", properties=properties
            )
        )
    ).trigger(once=True).start()

    return query.awaitTermination()


def write_to_postgres():
    spark = create_spark_session()
    df = create_initial_dataframe(spark)
    df_final = create_final_dataframe(df)
    start_streaming(df_final, spark=spark)


if __name__ == "__main__":
    write_to_postgres()
