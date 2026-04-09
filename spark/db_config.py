from dotenv import load_dotenv
import os

load_dotenv()
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_JAR = "/opt/postgresql-jdbc.jar"
BRONZE_TABLE = "bronze.request_raw"
SILVER_TABLE = "silver.request_cleaned"

POSTGRES_URL = (
    f"jdbc:postgresql://"
    f"{os.getenv('POSTGRES_HOST')}:"
    f"{os.getenv('POSTGRES_PORT')}/"
    f"{os.getenv('POSTGRES_DB')}"
)

POSTGRES_PROPS = {
    "user":     os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "driver":   "org.postgresql.Driver"
}

DB_CONFIG = {
    "host":     POSTGRES_HOST,
    "port":     os.getenv("POSTGRES_PORT"),
    "dbname":   os.getenv("POSTGRES_DB"),
    "user":     os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}