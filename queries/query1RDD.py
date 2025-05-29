import sys
import time
import logging
from config import HDFS_PARQUET_PATH, HDFS_BASE_RESULT_PATH_Q1_RDD, QUERY_1_RDD
from commonFunction import save_execution_time, create_spark_session
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

from queries.commonFunction import write_rdd_hdfs

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

schema = StructType([
    StructField("Country", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("CarbonDirect", DoubleType(), True),
    StructField("CFEpercent", DoubleType(), True),
])


def extract_year_and_transform(row):
    """Solo parsing dell'anno - nessun filtro necessario"""
    try:
        year = int(row.event_time[:4])
        return (row.Country, year), (row.CarbonDirect, row.CFEpercent)
    except (ValueError, TypeError):
        return None


def main(workers_number: int):
    spark = create_spark_session("Q1 Energy Stats RDD", "RDD", workers_number)

    start_time = time.time()

    rdd = (spark.read.schema(schema).parquet(HDFS_PARQUET_PATH).rdd
           .map(extract_year_and_transform)
           .filter(lambda x: x is not None))

    def seq_op(acc, val):
        """Operazione sequenziale ottimizzata"""
        if acc is None:
            # Prima volta: inizializza con i valori correnti
            carbon, cfe = val
            return [carbon, carbon, carbon, 1, cfe, cfe, cfe]  # sum, min, max, count, sum, min, max

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
        """Combinazione partizioni ottimizzata"""
        if a is None: return b
        if b is None: return a

        return [
            a[0] + b[0],  # sum carbon
            min(a[1], b[1]),  # min carbon
            max(a[2], b[2]),  # max carbon
            a[3] + b[3],  # count
            a[4] + b[4],  # sum cfe
            min(a[5], b[5]),  # min cfe
            max(a[6], b[6])  # max cfe
        ]

    # 3) Aggregazione con partitioning ottimale
    aggregated = rdd.aggregateByKey(None, seq_op, comb_op)

    # 4) Calcolo finale e ordinamento

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

    final_time = time.time() - start_time

    header = ("Country,record_year,avg_carbon_intensity,min_carbon_intensity,max_carbon_intensity,avg_cfe_percentage,"
              "min_cfe_percentage,max_cfe_percentage")

    write_rdd_hdfs(spark, header, result_rdd, HDFS_BASE_RESULT_PATH_Q1_RDD)

    save_execution_time(QUERY_1_RDD, workers_number, final_time)
    spark.stop()


if __name__ == "__main__":
    workers_number = int(sys.argv[1])
    main(workers_number)
