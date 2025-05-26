# query2.py
import time
import logging
from pyspark.sql import functions as F
from config import HDFS_PARQUET_PATH, HDFS_BASE_RESULT_PATH_Q2, QUERY_2
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from commonFunction import create_spark_session, save_execution_time

# Configura il logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

schema = StructType([
    StructField("record_year", IntegerType(), True),
    StructField("record_month", IntegerType(), True),
    StructField("CarbonDirect", DoubleType(), True),
    StructField("CFEpercent", DoubleType(), True),
])


# Query 2: Considerando il solo dataset italiano, aggregare i dati sulla coppia (anno, mese), calcolando il valor
# medio di “Carbon intensity gCO2eq/kWh (direct)” e “Carbon-free energy percentage (CFE%)”. Calcolare
# la classifica delle prime 5 coppie (anno, mese) ordinando per“Carbon intensity gCO2eq/kWh
# (direct)” decrescente, crescente e “Carbon-free energy percentage (CFE%)” decrescente, crescente.
# In totale sono attesi 20 valori.

def main():
    spark = create_spark_session("Q2 Energy Stats")

    # 1) Lettura dati Parquet
    start_read = time.time()
    df = spark.read.schema(schema).parquet(HDFS_PARQUET_PATH).filter(F.col("Country") == 'IT')
    read_time = time.time() - start_read
    logger.info(f"Tempo lettura Parquet: {read_time:.10f}s")
    df.explain(extended=True)

    start_query = time.time()
    result = (df.groupBy("record_year", "record_month").agg(
        F.avg("CarbonDirect").alias("avg_carbon_intensity"),
        F.avg("CFEpercent").alias("avg_cfe_percentage"),
    ).cache())


    result_carbon_desc = result.orderBy(F.col("avg_carbon_intensity").desc()).limit(5)
    result_carbon_asc = result.orderBy(F.col("avg_carbon_intensity").asc()).limit(5)
    cfe_percentage_desc = result.orderBy(F.col("avg_cfe_percentage").desc()).limit(5)
    cfe_percentage_asc = result.orderBy(F.col("avg_cfe_percentage").asc()).limit(5)

    query_time = time.time() - start_query
    logger.info(f"Tempo esecuzione query: {query_time:.10f}s")


    start_write = time.time()
    all_results = result_carbon_desc.union(result_carbon_asc).union(cfe_percentage_desc).union(cfe_percentage_asc)

    # 3) Scrittura risultati
    (all_results
     .coalesce(1)
     .write
     .mode("overwrite")
     .option("header", True)
     .csv(HDFS_BASE_RESULT_PATH_Q2))
    write_time = time.time() - start_write
    total_time = read_time + query_time + write_time
    logger.info(f"Tempo scrittura risultati: {write_time:.10f}s")
    logger.info(f"Tempi: \nTempo di lettura: {read_time}\nTempo di query: {query_time}\nTempo di write: {write_time}")
    logger.info(f"Tempo totale (read+query+write): {total_time:.10f}s")

    save_execution_time(QUERY_2, read_time, query_time, write_time, total_time)

    spark.stop()


if __name__ == "__main__":
    main()
