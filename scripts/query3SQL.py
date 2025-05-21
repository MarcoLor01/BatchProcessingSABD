import logging
import time
from scripts.commonFunction import create_spark_session, save_execution_time
from scripts.config import HDFS_PARQUET_PATH, QUERY_3_SQL, HDFS_BASE_RESULT_PATH_Q3_SQL
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# Configura il logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

schema = StructType([
    StructField("Country", StringType(), True),
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
    spark = create_spark_session("Q3 SQL Energy Stats")

    # 1) Lettura dati Parquet
    start_read = time.time()
    df = spark.read.schema(schema).parquet(HDFS_PARQUET_PATH)
    read_time = time.time() - start_read
    logger.info(f"Tempo lettura Parquet: {read_time:.10f}s")

    # Registra il DataFrame come vista temporanea per SQL
    df.createOrReplaceTempView("energy_data")

    # Query SQL migliorata che usa la vista temporanea invece di path hardcoded
    sql = """
    WITH hourly AS (
      SELECT
        Country,
        record_hour,
        AVG(CarbonDirect) AS avg_hour_carbon_intensity,
        AVG(CFEpercent)   AS avg_hour_cfe_percentage
      FROM energy_data
      WHERE Country IN ('IT','SE')
      GROUP BY Country, record_hour
    ),
    stats_carbon AS (
      SELECT
        Country,
        'carbon-intensity' AS metric,
        MIN(avg_hour_carbon_intensity)                                             AS min_val,
        percentile(avg_hour_carbon_intensity, ARRAY(0.25))[0]                       AS perc_25,
        percentile(avg_hour_carbon_intensity, ARRAY(0.50))[0]                       AS perc_50,
        percentile(avg_hour_carbon_intensity, ARRAY(0.75))[0]                       AS perc_75,
        MAX(avg_hour_carbon_intensity)                                             AS max_val
      FROM hourly
      GROUP BY Country
    ),
    stats_cfe AS (
      SELECT
        Country,
        'cfe' AS metric,
        MIN(avg_hour_cfe_percentage)                                              AS min_val,
        percentile(avg_hour_cfe_percentage, ARRAY(0.25))[0]                        AS perc_25,
        percentile(avg_hour_cfe_percentage, ARRAY(0.50))[0]                        AS perc_50,
        percentile(avg_hour_cfe_percentage, ARRAY(0.75))[0]                        AS perc_75,
        MAX(avg_hour_cfe_percentage)                                              AS max_val
      FROM hourly
      GROUP BY Country
    )
    SELECT * FROM stats_carbon
    UNION ALL
    SELECT * FROM stats_cfe
    """

    start_query = time.time()
    result = spark.sql(sql)
    query_time = time.time() - start_query
    logger.info(f"Tempo esecuzione SQL: {query_time:.5f}s")

    # 3) Mostra e salva
    result.show(20, truncate=False)

    start_write = time.time()
    (result
     .coalesce(1)
     .write
     .mode("overwrite")
     .option("header", True)
     .csv(HDFS_BASE_RESULT_PATH_Q3_SQL))

    write_time = time.time() - start_write
    total_time = read_time + query_time + write_time

    logger.info(f"Tempi — read: {read_time:.5f}s, query: {query_time:.5f}s, write: {write_time:.5f}s")
    logger.info(f"Tempo totale: {total_time:.5f}s")

    save_execution_time(QUERY_3_SQL, read_time, query_time, write_time, total_time)

    spark.stop()


if __name__ == "__main__":
    main()