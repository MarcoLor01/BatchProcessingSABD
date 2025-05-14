# query3.py
import time
import logging
from pyspark.sql import functions as F
from config import HDFS_PARQUET_PATH, HDFS_BASE_RESULT_PATH_Q2, QUERY_3
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from commonFunction import create_spark_session, save_execution_time

# Configura il logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

schema = StructType([
    StructField("Country", StringType(), True),
    StructField("hour", IntegerType(), True),
    StructField("carbon_intensity", DoubleType(), True),
    StructField("cfe_percentage", DoubleType(), True),
])


# Facendo riferimento al dataset dei valori energetici dell’Italia e della Svezia, aggregare i dati di ciascun
# paese sulle 24 ore della giornata, calcolando il valor medio di “Carbon intensity gCO2eq/kWh
# (direct)” e “Carbon-free energy percentage (CFE%)”. Calcolare il minimo, 25-esimo, 50-esimo, 75-
# esimo percentile e massimo del valor medio di “Carbon intensity gCO2eq/kWh (direct)” e “Carbonfree
# energy percentage (CFE%)”.

def main():
    spark = create_spark_session("Q3 Energy Stats")

    # 1) Lettura dati Parquet
    start_read = time.time()
    df = spark.read.parquet(HDFS_PARQUET_PATH)
    print("Numero di righe:", df.count())
    read_time = time.time() - start_read
    logger.info(f"Tempo lettura Parquet: {read_time:.10f}s")
    df.show()
    df.explain(extended=True)

    query_time = time.time()
    # 2. Calcola la media oraria per ciascun Paese
    intermediate_result = df.groupBy("Country", "hour").agg(
        F.avg("carbon_intensity").alias("avg_hour_carbon_intensity"),
        F.avg("cfe_percentage").alias("avg_hour_cfe_percentage")
    )

    # 2. Percentili per "carbon_intensity"
    carbon_intensity_stats = intermediate_result.groupBy("Country").agg(
        F.expr("percentile_approx(avg_hour_carbon_intensity, array(0.0, 0.25, 0.5, 0.75, 1.0), 10)").alias(
            "percentiles")
    ).select(
        F.col("Country"),
        F.lit("carbon-intensity").alias("data"),
        F.col("percentiles")[0].alias("min"),
        F.col("percentiles")[1].alias("perc_25"),
        F.col("percentiles")[2].alias("perc_50"),
        F.col("percentiles")[3].alias("perc_75"),
        F.col("percentiles")[4].alias("max")
    )

    # 3. Percentili per "cfe_percentage"
    cfe_stats = intermediate_result.groupBy("Country").agg(
        F.expr("percentile_approx(avg_hour_cfe_percentage, array(0.0, 0.25, 0.5, 0.75, 1.0), 10000)").alias(
            "percentiles")
    ).select(
        F.col("Country"),
        F.lit("cfe").alias("data"),
        F.col("percentiles")[0].alias("min"),
        F.col("percentiles")[1].alias("perc_25"),
        F.col("percentiles")[2].alias("perc_50"),
        F.col("percentiles")[3].alias("perc_75"),
        F.col("percentiles")[4].alias("max")
    )

    total_query_time = time.time() - query_time
    logger.info(f"Tempo necessario per la query: {total_query_time}")

    write_time_start = time.time()
    # 4. Unione dei risultati finali e scrittura
    final_result = carbon_intensity_stats.unionByName(cfe_stats)

    (final_result
     .coalesce(1)
     .write
     .mode("overwrite")
     .option("header", True)
     .csv(HDFS_BASE_RESULT_PATH_Q2))
    write_time = time.time() - write_time_start
    total_time = read_time + total_query_time + write_time
    logger.info(f"Tempo scrittura risultati: {write_time:.10f}s")
    logger.info(
        f"Tempi: \nTempo di lettura: {read_time}\nTempo di query: {total_query_time}\nTempo di write: {write_time}")
    logger.info(f"Tempo totale (read+query+write): {total_time:.10f}s")

    save_execution_time(QUERY_3, read_time, total_query_time, write_time, total_time)

    spark.stop()


if __name__ == "__main__":
    main()
