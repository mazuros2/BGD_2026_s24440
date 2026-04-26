from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from datetime import datetime, timedelta
import os

FEATURE_FLAG = os.getenv("FEATURE_FLAG")
KAFKA_FLAG = "kafka"

class KafkaSensor(BaseSensorOperator):

    @apply_defaults
    def __init__(self, topic: str, bootstrap_servers: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers

    def poke(self, context):
        from kafka import KafkaConsumer
        from kafka import TopicPartition

        try:
            consumer = KafkaConsumer(
                bootstrap_servers   = self.bootstrap_servers,
                consumer_timeout_ms = 3000,
            )

            partitions = consumer.partitions_for_topic(self.topic)

            if not partitions:
                consumer.close()
                return False

            topic_partitions = [TopicPartition(self.topic, p) for p in partitions]
            end_offsets = consumer.end_offsets(topic_partitions)

            total_messages = sum(end_offsets.values())

            consumer.close()

            if total_messages > 0:
                return True

            return False

        except Exception as e:
            return False

default_args = {
    "owner":       "nyc_311",
    "retries":     1,
    "retry_delay": timedelta(seconds=15),
    "start_date":  datetime(2026, 4, 5),
}

with DAG(
    dag_id           = "nyc_311_pipeline",
    default_args     = default_args,
    schedule_interval= None,
    catchup          = False,
    description      = "NYC 311: kafka or batch -> bronze -> silver -> gold tables -> gold request table -> gold aggregations",
    tags             = ["NYC 311 pipeline"],
) as dag:
    bronze = BashOperator(
        task_id      = "bronze_load",
        bash_command = "cd /opt/airflow/spark && python bronze_load.py",
    )

    silver = BashOperator(
        task_id      = "silver_transform",
        bash_command = "cd /opt/airflow/spark && python silver_transform.py",
    )

    gold_tables = BashOperator(
        task_id      = "gold_tables",
        bash_command = "cd /opt/airflow/spark && python gold_tables.py",
    )

    gold_requests_table = BashOperator(
        task_id      = "gold_requests_table",
        bash_command = "cd /opt/airflow/spark && python gold_requests_table.py",
    )

    gold_agg = BashOperator(
        task_id      = "gold_aggregations",
        bash_command = "cd /opt/airflow/spark && python gold_aggregations.py",
    )

    if FEATURE_FLAG == KAFKA_FLAG:
        produce = BashOperator(
            task_id="kafka_produce",
            bash_command="cd /opt/airflow/kafka && python producer.py",
        )

        wait_for_kafka = KafkaSensor(
            task_id="wait_for_kafka_messages",
            topic="nyc_311_raw",
            bootstrap_servers="kafka:9092",
            poke_interval=15,
            timeout=600,
            mode="poke",
        )

        produce >> wait_for_kafka >> bronze >> silver >> gold_tables >> gold_requests_table >> gold_agg
    else:
        bronze >> silver >> gold_tables >> gold_requests_table >> gold_agg