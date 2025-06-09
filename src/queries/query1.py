import string
import sys
import time
import logging
from pyspark.sql import functions as F
from src.utilities.config import (
    HDFS_PARQUET_PATH, HDFS_BASE_RESULT_PATH_Q1,
    HDFS_CSV_PATH, QUERY_1_PARQUET, QUERY_1_CSV
)
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

def main(data_format, workers_number):
    spark = create_spark_session("Q1 Energy Stats", "DF", workers_number)

    # ---------------- Start Misuration ----------------
    start_time = time.time()

    if data_format.lower() == "parquet":
        df = (
            spark.read.schema(schema)
            .parquet(HDFS_PARQUET_PATH)
            .withColumn("event_time", F.to_timestamp("event_time", "yyyy-MM-dd HH:mm:ss"))
            .withColumn("record_year", F.year("event_time"))
        )
    else:
        df = (
            spark.read.schema(schema)
            .option("header", "true")
            .option("sep", ",")
            .csv(HDFS_CSV_PATH)
            .withColumn("event_time", F.to_timestamp("event_time", "yyyy-MM-dd HH:mm:ss"))
            .withColumn("record_year", F.year("event_time"))
        )


    # 2) Aggregazione annuale
    result = (
        df.groupBy("Country", "record_year")
        .agg(
            F.avg("CarbonDirect").alias("avg_carbon_intensity"),
            F.min("CarbonDirect").alias("min_carbon_intensity"),
            F.max("CarbonDirect").alias("max_carbon_intensity"),
            F.avg("CFEpercent").alias("avg_cfe_percentage"),
            F.min("CFEpercent").alias("min_cfe_percentage"),
            F.max("CFEpercent").alias("max_cfe_percentage"),
        )
        .orderBy("Country", "record_year")
    )

    result.cache()
    result.count()
    final_time = time.time() - start_time
    # ---------------- End Misuration ----------------

    # 3) Scrittura risultati
    if data_format.lower() == "parquet":
        (
            result
            .coalesce(1)
            .write
            .mode("overwrite")
            .option("header", True)
            .csv(HDFS_BASE_RESULT_PATH_Q1 + "/parquet/")
        )
        save_execution_time(QUERY_1_PARQUET, workers_number, final_time)
    else:
        (
            result
            .coalesce(1)
            .write
            .mode("overwrite")
            .option("header", True)
            .csv(HDFS_BASE_RESULT_PATH_Q1 + "/csv/")
        )
        save_execution_time(QUERY_1_CSV, workers_number, final_time)

    spark.stop()

if __name__ == "__main__":
    workers_number = int(sys.argv[2])
    data_format = sys.argv[1]
    main(data_format, workers_number)
