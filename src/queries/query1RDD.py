import sys
import time
import logging
from src.utilities.config import HDFS_BASE_RESULT_PATH_Q1_RDD, QUERY_1_RDD, HDFS_CSV_PATH_SWE, HDFS_CSV_PATH_ITA, \
    SCHEMA_QUERY_1_RDD
from src.utilities.commonQueryFunction import save_execution_time, create_spark_session, write_rdd_hdfs

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def extract_year_and_transform(line):
    fields = line.split(",")
    country = fields[0]
    date_part = fields[1].split("-")
    year = int(date_part[0])
    carbonDirect = float(fields[2])
    CFEpercent = float(fields[3])
    return (country, year), (carbonDirect, CFEpercent)


def main(workers_number: int):
    spark = create_spark_session("Q1 Energy Stats RDD", "RDD", workers_number)

    # ---------------- Start Misuration ----------------
    start_time = time.perf_counter()

    # Preparazione dati
    rdd_italy = spark.sparkContext.textFile(HDFS_CSV_PATH_ITA).zipWithIndex().filter(lambda x: x[1] != 0).map(lambda x: x[0])
    rdd_swe = spark.sparkContext.textFile(HDFS_CSV_PATH_SWE).zipWithIndex().filter(lambda x: x[1] != 0).map(lambda x: x[0])
    rdd = rdd_italy.union(rdd_swe).map(extract_year_and_transform)

    def seq_op(acc, val):
        carbon, cfe = val
        acc[0] += carbon  # sum carbon
        acc[1] = min(acc[1], carbon)  # min carbon
        acc[2] = max(acc[2], carbon)  # max carbon
        acc[3] += 1  # count
        acc[4] += cfe  # sum cfe
        acc[5] = min(acc[5], cfe)  # min cfe
        acc[6] = max(acc[6], cfe)  # max cfe
        return acc

    def comb_op(a, b):
        return [
            a[0] + b[0],  # sum carbon
            min(a[1], b[1]),  # min carbon
            max(a[2], b[2]),  # max carbon
            a[3] + b[3],  # count
            a[4] + b[4],  # sum cfe
            min(a[5], b[5]),  # min cfe
            max(a[6], b[6])  # max cfe
        ]

    zero = [
    0.0,            # sum_carbon
    float("inf"),   # min_carbon
    float("-inf"),  # max_carbon
    0,              # count
    0.0,            # sum_cfe
    float("inf"),   # min_cfe
    float("-inf")   # max_cfe
    ]

    # Calcolo misure
    aggregated = rdd.aggregateByKey(zero, seq_op, comb_op)

    # Calcolo finale e ordinamento
    result_rdd = (aggregated
                  .sortByKey()
                  .map(lambda kv: (
        kv[0][0], kv[0][1],
        kv[1][0] / kv[1][3],  # avg carbon
        kv[1][1], kv[1][2],
        kv[1][4] / kv[1][3],  # avg cfe
        kv[1][5], kv[1][6]
    ))
                  )

    result_rdd.cache()
    result_rdd.count()
    final_time = time.perf_counter() - start_time
    print("Tempo finale: ", str(final_time))
    # ---------------- End Misuration ----------------

    write_rdd_hdfs(result_rdd,HDFS_BASE_RESULT_PATH_Q1_RDD,SCHEMA_QUERY_1_RDD)
    save_execution_time(QUERY_1_RDD, workers_number, final_time)
    spark.stop()


if __name__ == "__main__":
    workers_number = int(sys.argv[1])
    main(workers_number)
