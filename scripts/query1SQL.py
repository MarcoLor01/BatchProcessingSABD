import time
import logging
from config import HDFS_PARQUET_PATH, HDFS_BASE_RESULT_PATH_Q1, QUERY_1_SQL
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from scripts.commonFunction import save_execution_time, create_spark_session

# Configura il logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

schema = StructType([
    StructField("Country", StringType(), True),
    StructField("year", IntegerType(), True),
    StructField("carbon_intensity", DoubleType(), True),
    StructField("cfe_percentage", DoubleType(), True),
])


def main():
    spark = create_spark_session("Q1 Energy Stats SQL")

    # 1) Lettura dati Parquet
    start_read = time.time()
    df = spark.read.schema(schema).parquet(HDFS_PARQUET_PATH)
    df.createOrReplaceTempView("energy_data") #Per fare query SQL
    read_time = time.time() - start_read
    logger.info(f"Tempo lettura Parquet: {read_time:.10f}s")
    df.show()

    # 2) Aggregazione annuale con SQL
    start_query = time.time()

    sql_query = """
    SELECT 
        Country,
        year,
        AVG(carbon_intensity) AS avg_carbon_intensity,
        MIN(carbon_intensity) AS min_carbon_intensity,
        MAX(carbon_intensity) AS max_carbon_intensity,
        AVG(cfe_percentage) AS avg_cfe_percentage,
        MIN(cfe_percentage) AS min_cfe_percentage,
        MAX(cfe_percentage) AS max_cfe_percentage
    FROM 
        energy_data
    GROUP BY 
        Country, year
    ORDER BY 
        Country, year
    """

    result = spark.sql(sql_query)
    query_time = time.time() - start_query
    logger.info(f"Tempo esecuzione query: {query_time:.10f}s")

    # Stampa piano di esecuzione e DAG
    logger.info("=== Piano di esecuzione (logical & physical) ===")
    result.explain(extended=True)

    # 3) Scrittura risultati
    start_write = time.time()
    (result
     .coalesce(1)  # Vogliamo un unico CSV
     .write
     .mode("overwrite")
     .option("header", True)
     .csv(HDFS_BASE_RESULT_PATH_Q1))
    write_time = time.time() - start_write
    logger.info(f"Tempo scrittura risultati: {write_time:.10f}s")

    total = read_time + query_time + write_time
    logger.info(f"Tempi: \nTempo di lettura: {read_time}\nTempo di query: {query_time}\nTempo di write: {write_time}")
    logger.info(f"Tempo totale (read+query+write): {total:.10f}s")

    save_execution_time(QUERY_1_SQL, read_time, query_time, write_time, total)
    spark.stop()


if __name__ == "__main__":
    main()