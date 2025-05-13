# query2.py
import time
import logging
from pyspark.sql import SparkSession, functions as F
from config import HDFS_PARQUET_PATH, HDFS_BASE_RESULT_PATH_Q2

# Configura il logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

# Facendo riferimento al dataset dei valori energetici dell’Italia e della Svezia, aggregare i dati di ciascun
# paese sulle 24 ore della giornata, calcolando il valor medio di “Carbon intensity gCO2eq/kWh
# (direct)” e “Carbon-free energy percentage (CFE%)”. Calcolare il minimo, 25-esimo, 50-esimo, 75-
# esimo percentile e massimo del valor medio di “Carbon intensity gCO2eq/kWh (direct)” e “Carbonfree
# energy percentage (CFE%)”.

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
    df = spark.read.parquet(HDFS_PARQUET_PATH)
    read_time = time.time() - start_read
    logger.info(f"Tempo lettura Parquet: {read_time:.10f}s")
    df.explain(extended=True)

    # 2. Calcola la media oraria per ciascun Paese
    intermediate_result = df.groupBy("Country", "hour").agg(
        F.avg("carbon_intensity").alias("avg_hour_carbon_intensity"),
        F.avg("cfe_percentage").alias("avg_hour_cfe_percentage")
    )

    # 3. Calcola i percentili sulle 24 medie orarie per ogni Paese
    carbon_intensity_stats = intermediate_result.groupBy("Country").agg(
        F.expr("percentile_approx(avg_hour_carbon_intensity, array(0.0, 0.25, 0.5, 0.75, 1.0), 10000)")
        .alias("percentiles_ci")
    ).withColumn("data", F.lit("carbon-intensity"))

    cfe_stats = intermediate_result.groupBy("Country").agg(
        F.expr("percentile_approx(avg_hour_cfe_percentage, array(0.0, 0.25, 0.5, 0.75, 1.0), 10000)")
        .alias("percentiles_cfe")
    ).withColumn("data", F.lit("cfe"))

    # 4. Unisci i risultati
    final_result = carbon_intensity_stats.unionByName(cfe_stats)

    final_result.show()


if __name__ == "__main__":
    main()