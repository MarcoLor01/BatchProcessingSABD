import sys
import time
import logging
from config import HDFS_PARQUET_PATH, HDFS_BASE_RESULT_PATH_Q1_SQL, QUERY_1_SQL
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from queries.commonFunction import save_execution_time, create_spark_session
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
    spark = create_spark_session("Q1 Energy Stats SQL", "DF", workers_number)

    # 1) Lettura dati Parquet
    start_time = time.time()

    (spark.read.schema(schema).parquet(HDFS_PARQUET_PATH).withColumn("event_time",
    F.to_timestamp("event_time", "yyyy-MM-dd HH:mm:ss")).withColumn(
    "record_year", F.year("event_time"))
    .createOrReplaceTempView("energy_data"))

    sql_query = """
    SELECT 
        Country,
        record_year,
        AVG(CarbonDirect) AS avg_carbon_intensity,
        MIN(CarbonDirect) AS min_carbon_intensity,
        MAX(CarbonDirect) AS max_carbon_intensity,
        AVG(CFEpercent) AS avg_cfe_percentage,
        MIN(CFEpercent) AS min_cfe_percentage,
        MAX(CFEpercent) AS max_cfe_percentage
    FROM 
        energy_data
    GROUP BY 
        Country, record_year
    ORDER BY 
        Country, record_year
    """

    result = spark.sql(sql_query)

    final_time = time.time() - start_time

    # 3) Scrittura risultati
    (result
     .coalesce(1)  # Vogliamo un unico CSV
     .write
     .mode("overwrite")
     .option("header", True)
     .csv(HDFS_BASE_RESULT_PATH_Q1_SQL))

    save_execution_time(QUERY_1_SQL, workers_number, final_time)
    spark.stop()


if __name__ == "__main__":
    workers_number = int(sys.argv[1])
    main(workers_number)
