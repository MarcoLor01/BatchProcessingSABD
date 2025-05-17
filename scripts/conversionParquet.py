import time
import logging
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import TimestampType, DoubleType
from config import HDFS_BASE_PATH, HDFS_PARQUET_PATH, COUNTRIES, YEARS
import os

# Configura il logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def main():
    spark = (SparkSession.builder
             .appName("ETL CSV to Parquet")
             .config("spark.sql.parquet.filterPushdown", "true")
             .config("spark.sql.parquet.enableVectorizedReader", "true")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    start_time = time.time()

    # Lettura CSV, cast e scrittura Parquet partizionato
    paths = [f"{HDFS_BASE_PATH}/{country}/{year}.csv"
             for country in COUNTRIES for year in YEARS]

    df = (spark.read.option("header", True).option("encoding", "UTF-8").csv(paths).select(
        F.col("Country"),
        F.year("Datetime (UTC)").alias("year"),
        F.month("Datetime (UTC)").alias("month"),
        F.dayofmonth("Datetime (UTC)").alias("day"),
        F.hour("Datetime (UTC)").alias("hour"),
        F.col("Carbon intensity gCO₂eq/kWh (direct)").cast(DoubleType()).alias("carbon_intensity"),
        F.col("Carbon-free energy percentage (CFE%)").cast(DoubleType()).alias("cfe_percentage")
    ))

    print(df.count())

    output_path = HDFS_PARQUET_PATH

    df.write \
        .mode("overwrite") \
        .partitionBy("Country") \
        .parquet(output_path)

    elapsed = time.time() - start_time
    logger.info(f"Completato in {elapsed:.2f}s, output su {output_path}")
    spark.stop()


if __name__ == "__main__":
    main()
