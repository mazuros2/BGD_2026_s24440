import logging
import os
import json
import csv
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "nyc_311_raw")
CSV_PATH = os.getenv("CSV_PATH", "/opt/airflow/data/311_nyc_requests.csv")
BATCH_SIZE = 1000
MAX_ROWS = 50000

def create_topic():
    try:
        admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
        topic = NewTopic(
            name=KAFKA_TOPIC,
            num_partitions=3,
            replication_factor=1
        )
        admin.create_topics([topic])
        logger.info("topic: ", KAFKA_TOPIC)
    except TopicAlreadyExistsError:
        logger.info("topic already exists: ", KAFKA_TOPIC)
    finally:
        admin.close()

def produce_messages():
    producer = KafkaProducer(
        bootstrap_servers = KAFKA_BOOTSTRAP_SERVERS,
        value_serializer = lambda v: json.dumps(v).encode("utf-8"),
        key_serializer = lambda k: k.encode("utf-8") if k else None,
        batch_size = 65536,
        linger_ms = 50,
        compression_type = "gzip",
        request_timeout_ms = 60000,
        retries = 5,
    )

    sent = 0
    skipped = 0

    with open(CSV_PATH, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            if sent >= MAX_ROWS:
                break

            unique_key = row.get("Unique Key", "").strip()
            if not unique_key:
                skipped += 1
                continue

            producer.send(
                topic = KAFKA_TOPIC,
                key   = unique_key,
                value = row
            )

            sent += 1

            if sent % BATCH_SIZE == 0:
                producer.flush()

    producer.flush()
    producer.close()

    return sent

if __name__ == "__main__":
    create_topic()
    produce_messages()