from pyspark.sql import SparkSession
from db_config import POSTGRES_URL, POSTGRES_PROPS
from spark.db_config import BRONZE_TABLE

CSV_PATH = "/opt/airflow/data/311_nyc_requests.csv"

def run_bronze(spark):

    df = (
        spark.read
        .option("header",      True)
        .option("inferSchema", False)
        .option("multiLine",   True)
        .option("escape",      '"')
        .option("encoding",    "UTF-8")
        .csv(CSV_PATH)
    )

    df = df.toDF(
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
    )

    (
        df.write
        .mode("append")
        .option("batchsize", "10000")
        .jdbc(
            url        = POSTGRES_URL,
            table      = BRONZE_TABLE,
            properties = POSTGRES_PROPS
        )
    )

    return df

if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("nyc_311_bronze")
        .config("spark.driver.memory", "4g")
        .config("spark.jars", "/opt/postgresql-jdbc.jar")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    run_bronze(spark)
    spark.stop()