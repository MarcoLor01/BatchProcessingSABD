import logging
import sys
import time
from src.utilities.commonQueryFunction import create_spark_session, save_execution_time
from pyspark.sql import functions as F
from src.utilities.config import HDFS_PARQUET_PATH, QUERY_3_EXACT, HDFS_BASE_RESULT_PATH_Q3_EXACT
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

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


# Facendo riferimento al dataset dei valori energetici dell’Italia e della Svezia, aggregare i dati di ciascun
# paese sulle 24 ore della giornata, calcolando il valor medio di “Carbon intensity gCO2eq/kWh
# (direct)” e “Carbon-free energy percentage (CFE%)”. Calcolare il minimo, 25-esimo, 50-esimo, 75-
# esimo percentile e massimo del valor medio di “Carbon intensity gCO2eq/kWh (direct)” e “Carbonfree
# energy percentage (CFE%)”.

def main(workers_number: int):
    spark = create_spark_session("Q3 Exact Energy Stats", "DF", workers_number)

    # ---------------- Start Misuration ----------------
    start_read = time.perf_counter()

    df = spark.read.schema(schema).parquet(HDFS_PARQUET_PATH).withColumn("event_time",
    F.to_timestamp("event_time", "yyyy-MM-dd HH:mm:ss")).withColumn("record_hour", F.hour("event_time"))

    # 2. Calcola la media oraria per ciascun Paese
    intermediate_result = df.groupBy("Country", "record_hour").agg(
        F.avg("CarbonDirect").alias("avg_hour_carbon_intensity"),
        F.avg("CFEpercent").alias("avg_hour_cfe_percentage"),
        F.min("event_time").alias("hour_timestamp")
    )

    carbon_stats = intermediate_result.groupBy("Country").agg(
        F.min("avg_hour_carbon_intensity").alias("carbon_min"),
        F.expr("percentile(avg_hour_carbon_intensity, array(0.25, 0.5, 0.75))").alias("carbon_quartiles"),  # Corretto
        F.max("avg_hour_carbon_intensity").alias("carbon_max"),
        F.min("avg_hour_cfe_percentage").alias("cfe_min"),
        F.expr("percentile(avg_hour_cfe_percentage, array(0.25, 0.5, 0.75))").alias("cfe_quartiles"),
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

    final_result = carbon_result.unionByName(cfe_result)

    final_result.cache()
    final_result.count()
    total_time = time.perf_counter() - start_read
    # ---------------- End Misuration ----------------

    # === CSV 1: STATISTICHE DESCRITTIVE ===
    (final_result
     .coalesce(1)
     .write
     .mode("overwrite")
     .option("header", True)
     .csv(HDFS_BASE_RESULT_PATH_Q3_EXACT + "stats"))

    # === CSV 2: ANDAMENTO ORARIO (per grafici Grafana) ===
    (intermediate_result
     .orderBy("Country", "record_hour")
     .coalesce(1)
     .write
     .mode("overwrite")
     .option("header", True)
     .csv(HDFS_BASE_RESULT_PATH_Q3_EXACT + "hourly"))

    save_execution_time(QUERY_3_EXACT, workers_number, total_time)

    spark.stop()


if __name__ == "__main__":
    workers_number = int(sys.argv[1])
    main(workers_number)
