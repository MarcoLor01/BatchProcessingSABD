# query1.py
import sys
import time
import logging
from pyspark.sql import functions as F
from config import HDFS_PARQUET_PATH, HDFS_BASE_RESULT_PATH_Q1, QUERY_1
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from scripts.commonFunction import save_execution_time, create_spark_session

# Configura il logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

schema = StructType([
    StructField("Country", StringType(), True),
    StructField("record_year", IntegerType(), True),
    StructField("CarbonDirect", DoubleType(), True),
    StructField("CFEpercent", DoubleType(), True),
])


def main(workers_number: int):
    spark = create_spark_session("Q1 Energy Stats")

    # 1) Lettura dati Parquet
    start_read = time.time()
    df = spark.read.schema(schema).parquet(HDFS_PARQUET_PATH)
    read_time = time.time() - start_read
    logger.info(f"Tempo lettura Parquet: {read_time:.10f}s")
    df.show()
    # 2) Aggregazione annuale
    start_query = time.time()
    result = (df
              .groupBy("Country", "record_year")  # Wide
              .agg(
        F.avg("CarbonDirect").alias("avg_carbon_intensity"),
        F.min("CarbonDirect").alias("min_carbon_intensity"),
        F.max("CarbonDirect").alias("max_carbon_intensity"),
        F.avg("CFEpercent").alias("avg_cfe_percentage"),
        F.min("CFEpercent").alias("min_cfe_percentage"),
        F.max("CFEpercent").alias("max_cfe_percentage")
    ).orderBy("Country", "record_year"))  # Wide, necessario per riordinamento

    query_time = time.time() - start_query
    logger.info(f"Tempo esecuzione query: {query_time:.10f}s")
    result.show()

    # 3) Scrittura risultati
    start_write = time.time()
    (result
     .coalesce(1)  # Vogliamo un unico CSV
     .write
     .mode("overwrite")
     .option("header", True)
     .csv(HDFS_BASE_RESULT_PATH_Q1))
    write_time = time.time() - start_write
    logger.info(f"Tempo scrittura risultati: {write_time:.10f}s")

    total = read_time + query_time + write_time
    logger.info(f"Tempi: \nTempo di lettura: {read_time}\nTempo di query: {query_time}\nTempo di write: {write_time}")
    logger.info(f"Tempo totale (read+query+write): {total:.10f}s")

    save_execution_time(QUERY_1, workers_number, read_time, query_time, write_time, total)

    spark.stop()


if __name__ == "__main__":
    workers_number = int(sys.argv[1])
    main(workers_number)
