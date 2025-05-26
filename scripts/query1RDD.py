import sys
import time
import logging
from math import inf
from config import HDFS_PARQUET_PATH, HDFS_BASE_RESULT_PATH_Q1_RDD, QUERY_1_RDD
from scripts.commonFunction import save_execution_time, create_spark_session
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# Configura il logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

schema = StructType([
    StructField("Country", StringType(), True),
    StructField("record_year", IntegerType(), True),
    StructField("CarbonDirect", DoubleType(), True),
    StructField("CFEpercent", DoubleType(), True),
])


def main(workers_number: int):
    spark = create_spark_session("Q1 Energy Stats RDD Optimized")

    # 1) Lettura dati Parquet
    start_read = time.time()
    df = spark.read.schema(schema).parquet(HDFS_PARQUET_PATH)
    # Filtra record con valori null
    rdd = df.rdd
    read_time = time.time() - start_read
    logger.info(f"Tempo lettura Parquet: {read_time:.4f}s")

    start_query = time.time()

    pairs = rdd.map(lambda row: ( # Trasformazione
        (row.Country, row.record_year),  # Key
        (row.CarbonDirect,  # Per media
         row.CFEpercent,  # Per media
         1,
         row.CarbonDirect,  # Per min
         row.CarbonDirect,  # Per max
         row.CFEpercent,  # Per min
         row.CFEpercent)  # Per max
    ))

    # Tuple iniziale per ogni chiave
    zero = (0.0, 0.0, 0, inf, -inf, inf, -inf)

    def seq_op(acc, val):  # Ogni val viene combinato in acc
        sumC, sumF, cnt, minC, maxC, minF, maxF = acc
        c, f, _, minc0, maxc0, minf0, maxf0 = val
        return (
            sumC + c,
            sumF + f,
            cnt + 1,
            min(minC, c),
            max(maxC, c),
            min(minF, f),
            max(maxF, f)
        )

    def comb_op(a, b):  # Uniamo stati di partizioni diverse
        return (
            a[0] + b[0],
            a[1] + b[1],
            a[2] + b[2],
            min(a[3], b[3]),
            max(a[4], b[4]),
            min(a[5], b[5]),
            max(a[6], b[6])
        )

    # aggregateByKey ci permette di eseguire operazione su coppie kv, parametri:
    # valore zero: stato iniziale
    # seq_op: itero su tutte le coppie all'interno di una partizione ed eseguo la funzione
    # comb_op: combino a questo punto le varie partizioni

    aggregated = pairs.aggregateByKey(zero, seq_op, comb_op)

    # Calcola valori finali
    result_rdd = aggregated.map(lambda kv: (
        kv[0][0],  # Country
        kv[0][1],  # record_year
        kv[1][0] / kv[1][2],  # avg_carbon_intensity
        kv[1][3],  # min_carbon_intensity
        kv[1][4],  # max_carbon_intensity
        kv[1][1] / kv[1][2],  # avg_cfe_percentage
        kv[1][5],  # min_cfe_percentage
        kv[1][6]  # max_cfe_percentage
    ))  # RDD[(Country, year, avgC, minC, maxC, avgF, minF, maxF)]

    sorted_rdd = result_rdd.sortBy(lambda x: (x[0], x[1]))
    query_time = time.time() - start_query
    logger.info(f"Tempo esecuzione query: {query_time:.4f}s")

    # 3) Scrittura CSV
    start_write = time.time()
    header = (
            "Country,record_year,avg_carbon_intensity,min_carbon_intensity," +
            "max_carbon_intensity,avg_cfe_percentage,min_cfe_percentage,max_cfe_percentage"
    )
    csv_rdd = sorted_rdd.map(lambda row: ",".join(map(str, row)))
    final_rdd = spark.sparkContext.parallelize([header]).union(csv_rdd).coalesce(1)
    final_rdd.saveAsTextFile(HDFS_BASE_RESULT_PATH_Q1_RDD)
    write_time = time.time() - start_write
    logger.info(f"Tempo scrittura risultati: {write_time:.4f}s")

    total = read_time + query_time + write_time
    logger.info(f"Tempi (read/query/write): {read_time:.4f}/{query_time:.4f}/{write_time:.4f}s, totale {total:.4f}s")

    save_execution_time(QUERY_1_RDD, workers_number, read_time, query_time, write_time, total)
    spark.stop()


if __name__ == "__main__":
    workers_number = int(sys.argv[1])
    main(workers_number)
