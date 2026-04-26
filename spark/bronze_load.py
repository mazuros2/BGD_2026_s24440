from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from db_config import POSTGRES_URL, POSTGRES_PROPS, BRONZE_TABLE
from db_config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, CSV_PATH
from db_config import BATCH_FLAG, KAFKA_FLAG, FEATURE_FLAG

COLUMN_NAMES = [
    "unique_key", "created_date", "closed_date", "agency", "agency_name",
    "problem", "problem_detail", "additional_details", "location_type",
    "incident_zip", "incident_address", "street_name", "cross_street_1",
    "cross_street_2", "intersection_street_1", "intersection_street_2",
    "address_type", "city", "landmark", "facility_type", "status",
    "due_date", "resolution_description", "resolution_action_updated_date",
    "community_board", "council_district", "police_precinct", "bbl",
    "borough", "x_coordinate", "y_coordinate", "open_data_channel_type",
    "park_facility_name", "park_borough", "vehicle_type",
    "taxi_company_borough", "taxi_pickup_location", "bridge_highway_name",
    "bridge_highway_direction", "road_ramp", "bridge_highway_segment",
    "latitude", "longitude", "location"
]

def load_from_csv(spark: SparkSession) -> DataFrame:
    df = (
        spark.read
        .option("header",      True)
        .option("inferSchema", False)
        .option("multiLine",   True)
        .option("escape",      '"')
        .option("encoding",    "UTF-8")
        .csv(CSV_PATH)
    )

    df = df.toDF(*COLUMN_NAMES)
    return df

def load_from_kafka(spark: SparkSession) -> DataFrame:
    raw_df = (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe",               KAFKA_TOPIC)
        .option("startingOffsets",         "earliest")
        .option("endingOffsets",           "latest")
        .load()
    )

    json_strings = raw_df.select(F.col("value").cast("string").alias("json_value"))

    schema = spark.read.json(
        json_strings.limit(1000).rdd.map(lambda r: r.json_value)
    ).schema

    df = (
        json_strings
        .select(F.from_json("json_value", schema).alias("data"))
        .select("data.*")
    )

    df = df.toDF(*COLUMN_NAMES)
    return df


def run_bronze(spark: SparkSession) -> DataFrame:
    if FEATURE_FLAG == BATCH_FLAG:
        df = load_from_csv(spark)
    elif FEATURE_FLAG == KAFKA_FLAG:
        df = load_from_kafka(spark)
    else:
        raise ValueError(f"Unknown feature flag: '{FEATURE_FLAG}'. ")

    if df.head(1) == []:
        raise ValueError("no data")

    df.write \
      .mode("append") \
      .option("batchsize", "10000") \
      .jdbc(POSTGRES_URL, BRONZE_TABLE, properties=POSTGRES_PROPS)

    return df

if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("nyc_311_bronze")
        .config("spark.driver.memory", "4g")
        .config("spark.jars",
                "/opt/postgresql-jdbc.jar,"
                "/opt/spark-sql-kafka.jar,"
                "/opt/kafka-clients.jar,"
                "/opt/spark-token-provider-kafka.jar,"
                "/opt/commons-pool2.jar")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    run_bronze(spark)
    spark.stop()