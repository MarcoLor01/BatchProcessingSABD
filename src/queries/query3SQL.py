import logging
import sys
import time
from src.utilities.commonQueryFunction import create_spark_session, save_execution_time
from src.utilities.config import HDFS_PARQUET_PATH, QUERY_3_SQL_PARQUET, HDFS_CSV_PATH, QUERY_3_SQL_CSV, HDFS_BASE_RESULT_PATH_Q3_SQL
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql import functions as F

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

# Facendo riferimento al dataset dei valori energetici dell'Italia e della Svezia, aggregare i dati di ciascun
# paese sulle 24 ore della giornata, calcolando il valor medio di "Carbon intensity gCO2eq/kWh
# (direct)" e "Carbon-free energy percentage (CFE%)". Calcolare il minimo, 25-esimo, 50-esimo, 75-
# esimo percentile e massimo del valor medio di "Carbon intensity gCO2eq/kWh (direct)" e "Carbonfree
# energy percentage (CFE%)".

def main(data_format, workers_number):
    spark = create_spark_session("Q3 SQL Energy Stats", "DF", workers_number)

    # ---------------- Start Misuration ----------------
    start_read = time.perf_counter()

    if data_format.lower() == "parquet":
    	df = (
        spark.read.schema(schema)
        .parquet(HDFS_PARQUET_PATH)
        .withColumn("event_time", F.to_timestamp("event_time", "yyyy-MM-dd HH:mm:ss"))
        .withColumn("record_hour", F.hour("event_time"))
    )

    else:
    	df = (
        spark.read.schema(schema)
        .option("header", "true")
        .option("sep", ",")
        .option("recursiveFileLookup", "true")
        .csv(HDFS_CSV_PATH)
        .withColumn("event_time", F.to_timestamp("event_time", "yyyy-MM-dd HH:mm:ss"))
        .withColumn("record_hour", F.hour("event_time"))
    )
        
    df.createOrReplaceTempView("energy_data")

    # Query SQL
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

    result = spark.sql(sql)
    result.cache()
    result.count()
    final_time = time.perf_counter() - start_read
    # ---------------- End Misuration ----------------

    if data_format.lower() == "parquet":
        (result
         .coalesce(1)
         .write
         .mode("overwrite")
         .option("header", True)
         .csv(HDFS_BASE_RESULT_PATH_Q3_SQL + "/parquet/"))
        save_execution_time(QUERY_3_SQL_PARQUET, workers_number, final_time)
    else:
        (result
         .coalesce(1)
         .write
         .mode("overwrite")
         .option("header", True)
         .csv(HDFS_BASE_RESULT_PATH_Q3_SQL + "/csv/"))
        save_execution_time(QUERY_3_SQL_CSV, workers_number, final_time)

    spark.stop()


if __name__ == "__main__":
    workers_number = int(sys.argv[2])
    data_format = sys.argv[1]
    main(data_format, workers_number)
