import json
import logging
import os
import time

import psycopg2
from confluent_kafka import Consumer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
SCORES_TOPIC = os.getenv("KAFKA_SCORES_TOPIC", "scores")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "fraud_detection")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")


def connect_postgres():
    for attempt in range(30):
        try:
            connection = psycopg2.connect(
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                dbname=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
            )
            connection.autocommit = True
            logger.info("Connected to PostgreSQL")
            return connection
        except psycopg2.OperationalError as error:
            logger.warning("PostgreSQL is not ready (%s/30): %s", attempt + 1, error)
            time.sleep(2)
    raise RuntimeError("Could not connect to PostgreSQL")


def main():
    connection = connect_postgres()
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": "score-writer",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([SCORES_TOPIC])
    logger.info("Reading topic %s", SCORES_TOPIC)

    insert_sql = """
        INSERT INTO transaction_scores (transaction_id, score, fraud_flag)
        VALUES (%s, %s, %s)
    """

    try:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                logger.error("Kafka error: %s", message.error())
                continue
            try:
                payload = json.loads(message.value().decode("utf-8"))
                with connection.cursor() as cursor:
                    cursor.execute(
                        insert_sql,
                        (
                            payload["transaction_id"],
                            float(payload["score"]),
                            int(payload["fraud_flag"]),
                        ),
                    )
                logger.info("Saved transaction %s", payload["transaction_id"])
            except Exception as error:
                logger.error("Failed to save message: %s", error)
                connection.rollback()
    finally:
        consumer.close()
        connection.close()


if __name__ == "__main__":
    main()