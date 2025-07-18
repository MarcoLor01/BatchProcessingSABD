import sys
import time
import logging
from src.utilities.config import HDFS_BASE_RESULT_PATH_Q2_RDD, QUERY_2_RDD, HDFS_CSV_PATH_ITA, SCHEMA_QUERY_2_RDD
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from src.utilities.commonQueryFunction import create_spark_session, save_execution_time
from pyspark.sql import Row

from src.utilities.commonQueryFunction import write_rdd_hdfs

# Configura il logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


def extract_year_month_and_transform(line):
    fields = line.split(",")
    date_part = fields[1].split("-")
    year = int(date_part[0])
    month = int(date_part[1])
    carbonDirect = float(fields[2])
    CFEpercent = float(fields[3])
    return (year, month), (carbonDirect, CFEpercent, 1)


def main(workers_number: int):
    spark = create_spark_session("Q2 Energy Stats RDD", "RDD", workers_number)

    # ---------------- Start Misuration ----------------
    start_time = time.perf_counter()

    italy_rdd = (spark.sparkContext.textFile(HDFS_CSV_PATH_ITA).zipWithIndex()
                 .filter(lambda x: x[1] != 0)  # rimuove l'elemento con indice 0
                 .map(lambda x: x[0])  # prende solo il contenuto, non l'indice
                 .map(extract_year_month_and_transform))

    aggregated = italy_rdd.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1], a[2] + b[2]))

    result_rdd = (aggregated
                  .map(lambda kv: (
        kv[0],  # Anno, mese
        kv[1][0] / kv[1][2],  # Media CarbonDirect
        kv[1][1] / kv[1][2]  # Media CFEpercent
    )).cache())

    result_rdd.count()

    carbonDirectTopList = result_rdd.top(5, key=lambda x: x[1])
    carbonDirectBottomList = result_rdd.takeOrdered(5, key=lambda x: x[1])
    cfeTopList = result_rdd.top(5, key=lambda x: x[2])
    cfeBottomList = result_rdd.takeOrdered(5, key=lambda x: x[2])

    carbonDirectTopRdd = spark.sparkContext.parallelize([
        (item[0][0], item[0][1], item[1], item[2])
        for item in carbonDirectTopList
    ])

    carbonDirectBottomRdd = spark.sparkContext.parallelize([
        (item[0][0], item[0][1], item[1], item[2])
        for item in carbonDirectBottomList
    ])

    cfeTopRdd = spark.sparkContext.parallelize([
        (item[0][0], item[0][1], item[1], item[2])
        for item in cfeTopList
    ])

    cfeBottomRdd = spark.sparkContext.parallelize([
        (item[0][0], item[0][1], item[1], item[2])
        for item in cfeBottomList
    ])

    carbonDirectTopRdd.cache()
    carbonDirectBottomRdd.cache()
    cfeTopRdd.cache()
    cfeBottomRdd.cache()

    carbonDirectTopRdd.count()
    carbonDirectBottomRdd.count()
    cfeTopRdd.count()
    cfeBottomRdd.count()

    final_time = time.perf_counter() - start_time
    # ---------------- End Misuration ----------------

    write_rdd_hdfs(carbonDirectTopRdd, HDFS_BASE_RESULT_PATH_Q2_RDD + "CarbonDirectTop", SCHEMA_QUERY_2_RDD)
    write_rdd_hdfs(carbonDirectBottomRdd, HDFS_BASE_RESULT_PATH_Q2_RDD + "CarbonDirectBottom", SCHEMA_QUERY_2_RDD)
    write_rdd_hdfs(cfeTopRdd, HDFS_BASE_RESULT_PATH_Q2_RDD + "CfeTop", SCHEMA_QUERY_2_RDD)
    write_rdd_hdfs(cfeBottomRdd, HDFS_BASE_RESULT_PATH_Q2_RDD + "CfeBottom", SCHEMA_QUERY_2_RDD)

    save_execution_time(QUERY_2_RDD, workers_number, final_time)

    spark.stop()


if __name__ == "__main__":
    workers_number = int(sys.argv[1])
    main(workers_number)
