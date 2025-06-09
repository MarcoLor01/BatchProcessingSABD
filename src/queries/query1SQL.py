import sys
import time
import logging
from src.utilities.config import HDFS_PARQUET_PATH, HDFS_CSV_PATH, HDFS_BASE_RESULT_PATH_Q1_SQL, QUERY_1_SQL_CSV, QUERY_1_SQL_PARQUET
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from src.utilities.commonQueryFunction import save_execution_time, create_spark_session
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


def main(data_format, workers_number):
    spark = create_spark_session("Q1 Energy Stats SQL", "DF", workers_number)

    # ---------------- Start Misuration ----------------
    start_time = time.perf_counter()

    if data_format.lower() == "parquet":
        df = (
            spark.read.schema(schema)
            .parquet(HDFS_PARQUET_PATH)
            .withColumn("event_time", F.to_timestamp("event_time", "yyyy-MM-dd HH:mm:ss"))
            .withColumn("record_year", F.year("event_time")))
        
    else:
        df = (
            spark.read.schema(schema)
            .option("header", "true")
            .option("sep", ",")
            .option("recursiveFileLookup", "true")
            .csv(HDFS_CSV_PATH)
            .withColumn("event_time", F.to_timestamp("event_time", "yyyy-MM-dd HH:mm:ss"))
            .withColumn("record_year", F.year("event_time")))
        
    df.createOrReplaceTempView("energy_data")	
    

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
    result.cache()
    result.count()
    final_time = time.perf_counter() - start_time
    # ---------------- End Misuration ----------------


    # 3) Scrittura risultati
    if data_format.lower() == "parquet":
        (result
         .coalesce(1)  # Vogliamo un unico CSV
         .write
         .mode("overwrite")
         .option("header", True)
         .csv(HDFS_BASE_RESULT_PATH_Q1_SQL + "/parquet/"))
        save_execution_time(QUERY_1_SQL_PARQUET, workers_number, final_time)
    else:
        (result
         .coalesce(1)  # Vogliamo un unico CSV
         .write
         .mode("overwrite")
         .option("header", True)
         .csv(HDFS_BASE_RESULT_PATH_Q1_SQL + "/csv/"))
        save_execution_time(QUERY_1_SQL_CSV, workers_number, final_time)

    spark.stop()


if __name__ == "__main__":
    workers_number = int(sys.argv[2])
    data_format = sys.argv[1]
    main(data_format, workers_number)
