import sys
import time
import logging
from pyspark.sql import SparkSession
from config import HDFS_PARQUET_PATH, HDFS_BASE_RESULT_PATH_Q2_SQL, QUERY_2_SQL
from commonFunction import save_execution_time
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


def main(workers_number: int):
    spark = SparkSession.builder.appName("Q2 Energy Stats SQL").getOrCreate()

    # 1) Lettura e view
    start_time = time.time()

    (spark.read.schema(schema)
     .parquet(HDFS_PARQUET_PATH)
     .filter("Country = 'IT'")
     .withColumn("event_time", F.to_timestamp("event_time", "yyyy-MM-dd HH:mm:ss"))
     .withColumn("record_year", F.year("event_time"))
     .withColumn("record_month", F.month("event_time"))
     .createOrReplaceTempView("energy_it"))

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

    result = spark.sql(sql)
    final_time = time.time() - start_time

    (result
     .coalesce(1)
     .write
     .mode("overwrite")
     .option("header", True)
     .csv(HDFS_BASE_RESULT_PATH_Q2_SQL))

    save_execution_time(QUERY_2_SQL, workers_number, final_time)
    spark.stop()


if __name__ == "__main__":
    workers_number = int(sys.argv[1])
    main(workers_number)
