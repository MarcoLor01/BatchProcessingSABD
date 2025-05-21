import time
import logging
from pyspark.sql import SparkSession
from config import HDFS_PARQUET_PATH, HDFS_BASE_RESULT_PATH_Q2_SQL, QUERY_2_SQL
from commonFunction import save_execution_time
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# Configura il logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# Query 2: Considerando il solo dataset italiano, aggregare i dati sulla coppia (anno, mese), calcolando il valor
# medio di “Carbon intensity gCO2eq/kWh (direct)” e “Carbon-free energy percentage (CFE%)”. Calcolare
# la classifica delle prime 5 coppie (anno, mese) ordinando per“Carbon intensity gCO2eq/kWh
# (direct)” decrescente, crescente e “Carbon-free energy percentage (CFE%)” decrescente, crescente.
# In totale sono attesi 20 valori.

schema = StructType([
    StructField("record_year", IntegerType(), True),
    StructField("record_month", IntegerType(), True),
    StructField("CarbonDirect", DoubleType(), True),
    StructField("CFEpercent", DoubleType(), True),
])

def main():
    spark = SparkSession.builder.appName("Q2 Energy Stats SQL Fast").getOrCreate()

    # 1) Lettura e view
    start_read = time.time()
    (spark.read.schema(schema)
     .parquet(HDFS_PARQUET_PATH)
     .filter("Country = 'IT'")
     .createOrReplaceTempView("energy_it"))
    read_time = time.time() - start_read
    logger.info(f"Tempo lettura Parquet: {read_time:.5f}s")

    # 2) SQL
    sql = """
    WITH agg AS (
      SELECT
        record_year,
        record_month,
        AVG(CarbonDirect) AS avg_carbon_intensity,
        AVG(CFEpercent)   AS avg_cfe_percentage
      FROM energy_it
      GROUP BY record_year, record_month
    ),

    ranked AS (
      SELECT
        record_year,
        record_month,
        avg_carbon_intensity,
        avg_cfe_percentage,
        ROW_NUMBER() OVER (ORDER BY avg_carbon_intensity DESC) AS rn_carbon_desc,
        ROW_NUMBER() OVER (ORDER BY avg_carbon_intensity ASC)  AS rn_carbon_asc,
        ROW_NUMBER() OVER (ORDER BY avg_cfe_percentage DESC)   AS rn_cfe_desc,
        ROW_NUMBER() OVER (ORDER BY avg_cfe_percentage ASC)    AS rn_cfe_asc
      FROM agg
    )

    SELECT
      record_year,
      record_month,
      avg_carbon_intensity,
      avg_cfe_percentage,
      CASE
        WHEN rn_carbon_desc <= 5 THEN 'carbon_desc'
        WHEN rn_carbon_asc  <= 5 THEN 'carbon_asc'
        WHEN rn_cfe_desc     <= 5 THEN 'cfe_desc'
        WHEN rn_cfe_asc      <= 5 THEN 'cfe_asc'
      END AS metric
    FROM ranked
    WHERE rn_carbon_desc <= 5
       OR rn_carbon_asc  <= 5
       OR rn_cfe_desc     <= 5
       OR rn_cfe_asc      <= 5
    """
    start_query = time.time()
    result = spark.sql(sql)
    query_time = time.time() - start_query
    result.show()
    logger.info(f"Tempo esecuzione SQL fast: {query_time:.5f}s")

    # 3) Mostra e salva
    result.show(20, truncate=False)

    start_write = time.time()
    (result
     .coalesce(1)
     .write
     .mode("overwrite")
     .option("header", True)
     .csv(HDFS_BASE_RESULT_PATH_Q2_SQL))
    write_time = time.time() - start_write

    total_time = read_time + query_time + write_time
    logger.info(f"Tempi — read: {read_time:.5f}s, query: {query_time:.5f}s, write: {write_time:.5f}s")
    logger.info(f"Tempo totale: {total_time:.5f}s")

    save_execution_time(QUERY_2_SQL, read_time, query_time, write_time, total_time)
    spark.stop()


if __name__ == "__main__":
    main()
