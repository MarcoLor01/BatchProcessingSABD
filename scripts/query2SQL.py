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

schema = StructType([
    StructField("record_year", IntegerType(), True),
    StructField("record_month", IntegerType(), True),
    StructField("CarbonDirect", DoubleType(), True),
    StructField("CFEpercent", DoubleType(), True),
])


def main():
    spark = SparkSession.builder.appName("Q2 Energy Stats SQL").getOrCreate()

    # 1) Lettura e view
    start_read = time.time()
    (spark.read.schema(schema)
     .parquet(HDFS_PARQUET_PATH)
     .filter("Country = 'IT'")
     .createOrReplaceTempView("energy_it"))
    read_time = time.time() - start_read
    logger.info(f"Tempo lettura Parquet: {read_time:.5f}s")

    #spark.sql("""
    #      CACHE TABLE agg AS
    #      SELECT
    #        record_year,
    #        record_month,
    #        AVG(CarbonDirect) AS avg_carbon_intensity,
    #        AVG(CFEpercent)   AS avg_cfe_percentage
    #      FROM energy_it
    #      GROUP BY record_year, record_month
    #    """)

    # 2) SQL che replica esattamente la logica originale
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
    carbon_desc AS (
      SELECT record_year, record_month, avg_carbon_intensity, avg_cfe_percentage
      FROM agg
      ORDER BY avg_carbon_intensity DESC
      LIMIT 5
    ),
    carbon_asc AS (
      SELECT record_year, record_month, avg_carbon_intensity, avg_cfe_percentage
      FROM agg
      ORDER BY avg_carbon_intensity ASC
      LIMIT 5
    ),
    cfe_desc AS (
      SELECT record_year, record_month, avg_carbon_intensity, avg_cfe_percentage
      FROM agg
      ORDER BY avg_cfe_percentage DESC
      LIMIT 5
    ),
    cfe_asc AS (
      SELECT record_year, record_month, avg_carbon_intensity, avg_cfe_percentage
      FROM agg
      ORDER BY avg_cfe_percentage ASC
      LIMIT 5
    )
    SELECT * FROM carbon_desc
    UNION ALL
    SELECT * FROM carbon_asc
    UNION ALL
    SELECT * FROM cfe_desc
    UNION ALL
    SELECT * FROM cfe_asc
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
     .csv(HDFS_BASE_RESULT_PATH_Q2_SQL))
    write_time = time.time() - start_write

    total_time = read_time + query_time + write_time
    logger.info(f"Tempi — read: {read_time:.5f}s, query: {query_time:.5f}s, write: {write_time:.5f}s")
    logger.info(f"Tempo totale: {total_time:.5f}s")

    save_execution_time(QUERY_2_SQL, read_time, query_time, write_time, total_time)
    spark.stop()


if __name__ == "__main__":
    main()