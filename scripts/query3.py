# query3.py
import time
import logging
from pyspark.sql import functions as F
from config import HDFS_PARQUET_PATH, QUERY_3, HDFS_BASE_RESULT_PATH_Q3
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from commonFunction import create_spark_session, save_execution_time

# Configura il logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

# Schema corretto - include Country
schema = StructType([
    StructField("Country", StringType(), True),  # AGGIUNTO
    StructField("record_hour", IntegerType(), True),
    StructField("CarbonDirect", DoubleType(), True),
    StructField("CFEpercent", DoubleType(), True),
])


# Facendo riferimento al dataset dei valori energetici dell'Italia e della Svezia, aggregare i dati di ciascun
# paese sulle 24 ore della giornata, calcolando il valor medio di "Carbon intensity gCO2eq/kWh
# (direct)" e "Carbon-free energy percentage (CFE%)". Calcolare il minimo, 25-esimo, 50-esimo, 75-
# esimo percentile e massimo del valor medio di "Carbon intensity gCO2eq/kWh (direct)" e "Carbonfree
# energy percentage (CFE%)".

def main():
    spark = create_spark_session("Q3 Energy Stats")

    # 1) Lettura dati Parquet
    start_read = time.time()
    df = spark.read.schema(schema).parquet(HDFS_PARQUET_PATH)


    read_time = time.time() - start_read
    logger.info(f"Tempo lettura Parquet: {read_time:.10f}s")

    # 2) Inizio misurazione query
    start_query = time.time()

    intermediate_result = df.groupBy("Country", "record_hour").agg(
        F.avg("CarbonDirect").alias("avg_hour_carbon_intensity"),
        F.avg("CFEpercent").alias("avg_hour_cfe_percentage")
    )


    carbon_stats = intermediate_result.groupBy("Country").agg(
        F.min("avg_hour_carbon_intensity").alias("carbon_min"),
        F.expr("percentile_approx(avg_hour_carbon_intensity, array(0.25, 0.5, 0.75), 100)").alias("carbon_quartiles"),
        F.max("avg_hour_carbon_intensity").alias("carbon_max"),
        F.min("avg_hour_cfe_percentage").alias("cfe_min"),
        F.expr("percentile_approx(avg_hour_cfe_percentage, array(0.25, 0.5, 0.75), 100)").alias("cfe_quartiles"),
        F.max("avg_hour_cfe_percentage").alias("cfe_max")
    )

    # Reshape per il formato finale
    carbon_result = carbon_stats.select(
        F.col("Country"),
        F.lit("carbon-intensity").alias("data"),
        F.col("carbon_min").alias("min"),
        F.col("carbon_quartiles")[0].alias("perc_25"),
        F.col("carbon_quartiles")[1].alias("perc_50"),
        F.col("carbon_quartiles")[2].alias("perc_75"),
        F.col("carbon_max").alias("max")
    )

    cfe_result = carbon_stats.select(
        F.col("Country"),
        F.lit("cfe").alias("data"),
        F.col("cfe_min").alias("min"),
        F.col("cfe_quartiles")[0].alias("perc_25"),
        F.col("cfe_quartiles")[1].alias("perc_50"),
        F.col("cfe_quartiles")[2].alias("perc_75"),
        F.col("cfe_max").alias("max")
    )

    # Unione dei risultati finali
    final_result = carbon_result.unionByName(cfe_result)

    query_time = time.time() - start_query
    logger.info(f"Tempo necessario per la query: {query_time:.10f}s")

    # 3) Scrittura risultati
    start_write = time.time()
    (final_result
     .coalesce(1)
     .write
     .mode("overwrite")
     .option("header", True)
     .csv(HDFS_BASE_RESULT_PATH_Q3))

    write_time = time.time() - start_write
    total_time = read_time + query_time + write_time

    logger.info(f"Tempo scrittura risultati: {write_time:.10f}s")
    logger.info(f"Tempi: \nTempo di lettura: {read_time}\nTempo di query: {query_time}\nTempo di write: {write_time}")
    logger.info(f"Tempo totale (read+query+write): {total_time:.10f}s")

    save_execution_time(QUERY_3, read_time, query_time, write_time, total_time)
    spark.stop()


if __name__ == "__main__":
    main()