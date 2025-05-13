# query2.py
import time
import logging
from pyspark.sql import SparkSession, functions as F
from config import HDFS_PARQUET_PATH, HDFS_BASE_RESULT_PATH_Q2
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# Configura il logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


schema = StructType([
    StructField("year", IntegerType(), True),
    StructField("month", IntegerType(), True),
    StructField("carbon_intensity", DoubleType(), True),
    StructField("cfe_percentage", DoubleType(), True),
])

# Query 2: Considerando il solo dataset italiano, aggregare i dati sulla coppia (anno, mese), calcolando il valor
# medio di “Carbon intensity gCO2eq/kWh (direct)” e “Carbon-free energy percentage (CFE%)”. Calcolare
# la classifica delle prime 5 coppie (anno, mese) ordinando per“Carbon intensity gCO2eq/kWh
# (direct)” decrescente, crescente e “Carbon-free energy percentage (CFE%)” decrescente, crescente.
# In totale sono attesi 20 valori.

def main():
    spark = (SparkSession.builder
             .appName("Q2 Energy Stats - Parquet")
             .config("spark.sql.files.maxPartitionBytes", "128MB")
             .config("spark.sql.parquet.filterPushdown", "true")
             .config("spark.sql.parquet.enableVectorizedReader", "true")
             .config("spark.sql.adaptive.enabled", "true")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    # 1) Lettura dati Parquet
    start_read = time.time()
    df = spark.read.schema(schema).parquet(HDFS_PARQUET_PATH).filter(F.col("Country") == 'Italy')
    read_time = time.time() - start_read
    logger.info(f"Tempo lettura Parquet: {read_time:.10f}s")
    df.explain(extended=True)

    start_query = time.time()
    result = (df.groupBy("year", "month").agg(
        F.avg("carbon_intensity").alias("avg_carbon_intensity"),
        F.avg("cfe_percentage").alias("avg_cfe_percentage"),
    ))

    result_carbon_desc = result.orderBy(F.col("avg_carbon_intensity").desc()).limit(5)
    result_carbon_asc = result.orderBy(F.col("avg_carbon_intensity").asc()).limit(5)
    cfe_percentage_desc = result.orderBy(F.col("avg_cfe_percentage").desc()).limit(5)
    cfe_percentage_asc = result.orderBy(F.col("avg_cfe_percentage").asc()).limit(5)

    query_time = time.time() - start_query
    logger.info(f"Tempo esecuzione query: {query_time:.10f}s")
    result_carbon_desc.show()
    result_carbon_asc.show()
    cfe_percentage_desc.show()
    cfe_percentage_asc.show()

    all_results = result_carbon_desc.union(result_carbon_asc).union(cfe_percentage_desc).union(cfe_percentage_asc)

    # 3) Scrittura risultati
    start_write = time.time()
    (all_results
     .coalesce(1)
     .write
     .mode("overwrite")
     .option("header", True)
     .csv(HDFS_BASE_RESULT_PATH_Q2))
    write_time = time.time() - start_write
    logger.info(f"Tempo scrittura risultati: {write_time:.10f}s")
    all_results.show()
    spark.stop()


if __name__ == "__main__":
    main()
