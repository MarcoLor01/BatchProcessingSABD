# query1.py
import sys
import time
import logging
from pyspark.sql import functions as F
from src.utilities.config import HDFS_PARQUET_PATH, HDFS_BASE_RESULT_PATH_Q1, QUERY_1
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType
from src.utilities.commonQueryFunction import save_execution_time, create_spark_session

# Configura il logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

schema = StructType([
    StructField("Country", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("CarbonDirect", DoubleType(), True),
    StructField("CFEpercent", DoubleType(), True),
])


def main(workers_number: int):
    spark = create_spark_session("Q1 Energy Stats", "DF", workers_number)

    # ---------------- Start Misuration ----------------
    start_time = time.time()

    df = (spark.read.schema(schema).parquet(HDFS_PARQUET_PATH).withColumn("event_time",
     F.to_timestamp("event_time", "yyyy-MM-dd HH:mm:ss")).withColumn("record_year", F.year("event_time")))

    # 2) Aggregazione annuale
    result = (df
              .groupBy("Country", "record_year")  # Wide
              .agg(
        F.avg("CarbonDirect").alias("avg_carbon_intensity"),
        F.min("CarbonDirect").alias("min_carbon_intensity"),
        F.max("CarbonDirect").alias("max_carbon_intensity"),
        F.avg("CFEpercent").alias("avg_cfe_percentage"),
        F.min("CFEpercent").alias("min_cfe_percentage"),
        F.max("CFEpercent").alias("max_cfe_percentage"),
    ).orderBy("Country", "record_year"))  # Wide, necessario per riordinamento

    result.count()
    final_time = time.time() - start_time
    # ---------------- End Misuration ----------------


    # 3) Scrittura risultati
    (result
     .coalesce(1)  # Vogliamo un unico CSV
     .write
     .mode("overwrite")
     .option("header", True)
     .csv(HDFS_BASE_RESULT_PATH_Q1))

    save_execution_time(QUERY_1, workers_number, final_time)

    spark.stop()


if __name__ == "__main__":
    workers_number = int(sys.argv[1])
    main(workers_number)
