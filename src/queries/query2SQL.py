import sys
import time
import logging
from pyspark.sql import SparkSession
from src.utilities.config import HDFS_PARQUET_PATH, HDFS_BASE_RESULT_PATH_Q2_SQL, QUERY_2_SQL
from src.utilities.commonQueryFunction import save_execution_time
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

     # ---------------- Start Misuration ----------------
    start_time = time.time()

    (spark.read.schema(schema)
     .parquet(HDFS_PARQUET_PATH)
     .filter("Country = 'IT'")
     .withColumn("event_time", F.to_timestamp("event_time", "yyyy-MM-dd HH:mm:ss"))
     .withColumn("record_year", F.year("event_time"))
     .withColumn("record_month", F.month("event_time"))
     .createOrReplaceTempView("energy_it"))

    sql = """
    WITH agg AS (
      SELECT
        record_year,
        record_month,
        AVG(CarbonDirect) AS avg_carbon_intensity,
        AVG(CFEpercent)   AS avg_cfe_percentage
      FROM energy_it
      GROUP BY record_year, record_month
    )
    SELECT * FROM agg
    """

    # Cache
    agg_result = spark.sql(sql).cache()
    agg_result.count()
    agg_result.createOrReplaceTempView("agg_cached")

    final_sql = """
    SELECT *, 'carbon_desc' as type FROM agg_cached ORDER BY avg_carbon_intensity DESC LIMIT 5
    UNION ALL
    SELECT *, 'carbon_asc' as type FROM agg_cached ORDER BY avg_carbon_intensity ASC LIMIT 5
    UNION ALL  
    SELECT *, 'cfe_desc' as type FROM agg_cached ORDER BY avg_cfe_percentage DESC LIMIT 5
    UNION ALL
    SELECT *, 'cfe_asc' as type FROM agg_cached ORDER BY avg_cfe_percentage ASC LIMIT 5
    """

    result = spark.sql(final_sql)
    result.count()
    final_time = time.time() - start_time
     # ---------------- End Misuration ----------------

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
