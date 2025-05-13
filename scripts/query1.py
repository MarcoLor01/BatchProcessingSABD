# query1.py
import time
import logging
from pyspark.sql import SparkSession, functions as F
from config import HDFS_PARQUET_PATH, HDFS_BASE_RESULT_PATH_Q1
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# Configura il logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

schema = StructType([
    StructField("Country", StringType(), True),
    StructField("year", IntegerType(), True),
    StructField("carbon_intensity", DoubleType(), True),
    StructField("cfe_percentage", DoubleType(), True),
])


def main():
    spark = (SparkSession.builder
             .appName("Q1 Energy Stats - Parquet")
             .config("spark.sql.files.maxPartitionBytes", "128MB")
             .config("spark.sql.parquet.filterPushdown", "true")
             .config("spark.sql.parquet.enableVectorizedReader", "true")
             .config("spark.sql.adaptive.enabled", "true")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    # 1) Lettura dati Parquet
    start_read = time.time()
    df = spark.read.schema(schema).parquet(HDFS_PARQUET_PATH)
    read_time = time.time() - start_read
    logger.info(f"Tempo lettura Parquet: {read_time:.10f}s")
    df.show()
    # 2) Aggregazione annuale
    start_query = time.time()
    result = (df
              .groupBy("Country", "year")  # Wide
              .agg(
        F.avg("carbon_intensity").alias("avg_carbon_intensity"),
        F.min("carbon_intensity").alias("min_carbon_intensity"),
        F.max("carbon_intensity").alias("max_carbon_intensity"),
        F.avg("cfe_percentage").alias("avg_cfe_percentage"),
        F.min("cfe_percentage").alias("min_cfe_percentage"),
        F.max("cfe_percentage").alias("max_cfe_percentage")
    ).orderBy("Country", "year"))  # Wide, necessario per riordinamento

    query_time = time.time() - start_query
    logger.info(f"Tempo esecuzione query: {query_time:.10f}s")

    # Stampa piano di esecuzione e DAG
    logger.info("=== Piano di esecuzione (logical & physical) ===")
    result.explain(extended=True)

    # 3) Scrittura risultati
    start_write = time.time()
    (result
     .coalesce(1)
     .write
     .mode("overwrite")
     .option("header", True)
     .csv(HDFS_BASE_RESULT_PATH_Q1))
    write_time = time.time() - start_write
    logger.info(f"Tempo scrittura risultati: {write_time:.10f}s")

    total = read_time + query_time + write_time
    logger.info(f"Tempi: \nTempo di lettura: {read_time}\nTempo di query: {query_time}\nTempo di write: {write_time}")
    logger.info(f"Tempo totale (read+query+write): {total:.10f}s")

    spark.stop()


if __name__ == "__main__":
    main()
