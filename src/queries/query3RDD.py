import sys
import time
import logging
from src.utilities.config import HDFS_BASE_RESULT_PATH_Q3_RDD, QUERY_3_RDD, HDFS_CSV_PATH_SWE, HDFS_CSV_PATH_ITA, \
    SCHEMA_QUERY_3_RDD
from src.utilities.commonQueryFunction import create_spark_session, save_execution_time, write_rdd_hdfs

# Configura il logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


def calculate_percentile(sorted_data, p):
    n = len(sorted_data)
    if n == 0:
        return 0.0

    index = p * (n - 1)
    lower_index = int(index)
    upper_index = min(lower_index + 1, n - 1)

    if lower_index == upper_index:
        return sorted_data[lower_index]

    # Interpolazione lineare
    weight = index - lower_index
    return sorted_data[lower_index] * (1 - weight) + sorted_data[upper_index] * weight


def calculate_stats(values):
    sorted_values = sorted(values)

    return (
        sorted_values[0],  # min
        calculate_percentile(sorted_values, 0.25),  # 25%
        calculate_percentile(sorted_values, 0.50),  # 50%
        calculate_percentile(sorted_values, 0.75),  # 75%
        sorted_values[-1]  # max
    )


def extract_hour_and_transform(line):
    fields = line.split(",")
    datetime_str = fields[1].replace("-", " ").replace(":", " ")
    parts = datetime_str.split()  # ["2021", "01", "01", "00", "00", "00"]
    hour = int(parts[3])
    country = fields[0]
    carbonDirect = float(fields[2])
    CFEpercent = float(fields[3])
    return (country, hour), (carbonDirect, CFEpercent, 1)


def main(workers_number: int):
    spark = create_spark_session("Q3 Energy Stats RDD", "RDD", workers_number)

    # ---------------- Start Misuration ----------------
    start_read = time.perf_counter()

    rdd_italy = spark.sparkContext.textFile(HDFS_CSV_PATH_ITA).zipWithIndex().filter(lambda x: x[1] != 0).map(
        lambda x: x[0])
    rdd_swe = spark.sparkContext.textFile(HDFS_CSV_PATH_SWE).zipWithIndex().filter(lambda x: x[1] != 0).map(
        lambda x: x[0])
    base_rdd = rdd_italy.union(rdd_swe).map(extract_hour_and_transform)

    hourly_agg = base_rdd.reduceByKey(
        lambda a, b: (a[0] + b[0], a[1] + b[1], a[2] + b[2])
    )
	
    country_stats = (
        hourly_agg
        .map(lambda kv: (kv[0][0], (kv[1][0] / kv[1][2], kv[1][1] / kv[1][2])))
        .groupByKey()
        .mapValues(lambda values: (
            calculate_stats([v[0] for v in values]),  # Stats per carbon
            calculate_stats([v[1] for v in values])  # Stats per CFE
        ))
    )

    # Trasforma in formato finale
    results = country_stats.flatMap(lambda rec: [
        (rec[0], 'carbon-intensity', *rec[1][0]),  # carbon stats
        (rec[0], 'cfe', *rec[1][1])  # cfe stats
    ])
	
    results.cache()
    results.count()
    final_time = time.perf_counter() - start_read
    # ---------------- End Misuration ----------------

    write_rdd_hdfs(results, HDFS_BASE_RESULT_PATH_Q3_RDD, SCHEMA_QUERY_3_RDD)
    save_execution_time(QUERY_3_RDD, workers_number, final_time)
    spark.stop()


if __name__ == "__main__":
    workers_number = int(sys.argv[1])
    main(workers_number)
