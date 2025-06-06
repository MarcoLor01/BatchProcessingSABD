# query2.py
import sys
import time
import logging
from pyspark.sql import functions as F
from src.utilities.config import HDFS_PARQUET_PATH, HDFS_BASE_RESULT_PATH_Q2, QUERY_2
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from src.utilities.commonQueryFunction import create_spark_session, save_execution_time

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


# Query 2: Considerando il solo dataset italiano, aggregare i dati sulla coppia (anno, mese), calcolando il valor
# medio di “Carbon intensity gCO2eq/kWh (direct)” e “Carbon-free energy percentage (CFE%)”. Calcolare
# la classifica delle prime 5 coppie (anno, mese) ordinando per“Carbon intensity gCO2eq/kWh
# (direct)” decrescente, crescente e “Carbon-free energy percentage (CFE%)” decrescente, crescente.
# In totale sono attesi 20 valori.

def main(workers_number: int):
    spark = create_spark_session("Q2 Energy Stats", "DF", workers_number)

    # ---------------- Start Misuration ----------------
    start_time = time.perf_counter()
    df = (spark.read.schema(schema).parquet(HDFS_PARQUET_PATH).filter(F.col("Country") == 'IT').withColumn("event_time",
    F.to_timestamp("event_time", "yyyy-MM-dd HH:mm:ss")).withColumn("record_year", F.year("event_time"))
    .withColumn("record_month", F.month("event_time")))

    result = (df.groupBy("record_year", "record_month").agg(
        F.avg("CarbonDirect").alias("avg_carbon_intensity"),
        F.avg("CFEpercent").alias("avg_cfe_percentage"),
        F.min("event_time").alias("month_timestamp")
    ).cache())

    result.count()

    # === CSV 1: CLASSIFICHE (20 valori) ===
    result_carbon_desc = result.orderBy(F.col("avg_carbon_intensity").desc()).limit(5)
    result_carbon_asc = result.orderBy(F.col("avg_carbon_intensity").asc()).limit(5)
    cfe_percentage_desc = result.orderBy(F.col("avg_cfe_percentage").desc()).limit(5)
    cfe_percentage_asc = result.orderBy(F.col("avg_cfe_percentage").asc()).limit(5)
    
    result_carbon_desc.cache()
    result_carbon_asc.cache()
    cfe_percentage_desc.cache()
    cfe_percentage_asc.cache()
    
    result_carbon_desc.count()
    result_carbon_asc.count()
    cfe_percentage_desc.count()
    cfe_percentage_asc.count()

    final_time = time.perf_counter() - start_time
    # ---------------- End Misuration ----------------

    # Scrittura CSV 1 - Classifiche

    result_carbon_desc.coalesce(1).write.mode("overwrite").option("header", True).csv(
        HDFS_BASE_RESULT_PATH_Q2 + "result_carbon_desc")
    result_carbon_asc.coalesce(1).write.mode("overwrite").option("header", True).csv(
        HDFS_BASE_RESULT_PATH_Q2 + "result_carbon_asc")
    cfe_percentage_desc.coalesce(1).write.mode("overwrite").option("header", True).csv(
        HDFS_BASE_RESULT_PATH_Q2 + "cfe_percentage_desc")
    cfe_percentage_asc.coalesce(1).write.mode("overwrite").option("header", True).csv(
        HDFS_BASE_RESULT_PATH_Q2 + "cfe_percentage_asc")

    # === CSV 2: SERIE TEMPORALE COMPLETA (per grafico) ===

    result_timeseries = result.orderBy("month_timestamp")

    # Scrittura CSV 2 - Serie temporale
    (result_timeseries
     .coalesce(1)
     .write
     .mode("overwrite")
     .option("header", True)
     .csv(HDFS_BASE_RESULT_PATH_Q2 + "timeseries"))

    save_execution_time(QUERY_2, workers_number, final_time)

    spark.stop()


if __name__ == "__main__":
    workers_number = int(sys.argv[1])
    main(workers_number)
