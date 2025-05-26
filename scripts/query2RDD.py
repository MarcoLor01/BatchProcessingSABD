import sys
import time
import logging
from config import HDFS_PARQUET_PATH, HDFS_BASE_RESULT_PATH_Q2_RDD, QUERY_2_RDD
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from commonFunction import create_spark_session, save_execution_time

# Configura il logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

schema = StructType([
    StructField("Country", StringType(), True),
    StructField("record_year", IntegerType(), True),
    StructField("record_month", IntegerType(), True),
    StructField("CarbonDirect", DoubleType(), True),
    StructField("CFEpercent", DoubleType(), True),
])

# Query 2: Considerando il solo dataset italiano, aggregare i dati sulla coppia (anno, mese), calcolando il valor
# medio di “Carbon intensity gCO2eq/kWh (direct)” e “Carbon-free energy percentage (CFE%)”. Calcolare
# la classifica delle prime 5 coppie (anno, mese) ordinando per“Carbon intensity gCO2eq/kWh
# (direct)” decrescente, crescente e “Carbon-free energy percentage (CFE%)” decrescente, crescente.
# In totale sono attesi 20 valori.

def main(workers_number: int):
    spark = create_spark_session("Q2 Energy Stats RDD")

    # 1) Lettura dati Parquet
    start_read = time.time()
    df = spark.read.schema(schema).parquet(HDFS_PARQUET_PATH)
    df = df.filter(df.Country == 'IT')

    read_time = time.time() - start_read

    logger.info(f"Tempo lettura Parquet: {read_time:.10f}s")

    rdd = df.rdd
    query_initial_time = time.time()
    pairs = rdd.map(lambda row: ( # Trasformazione
    (row.record_year, row.record_month),
    (row.CarbonDirect, # Per media
     row.CFEpercent, # Per media
     1)
    ))

    zero = (0.0, 0.0, 0)

    def seq_op(acc, val):
        sumC, sumF, cnt = acc
        c, f, _ = val
        return (
            sumC + c,
            sumF + f,
            cnt + 1
        )

    def comb_op(a, b):
        return (
            a[0] + b[0],
            a[1] + b[1],
            a[2] + b[2],
        )

    aggregated = pairs.aggregateByKey(zero, seq_op, comb_op)

    result_rdd = aggregated.map(lambda kv: (
        kv[0][0], # Anno
        kv[0][1], # Mese
        kv[1][0] / kv[1][2],
        kv[1][1] / kv[1][2],
    )).cache() #RDD[(Anno, mese, avgC, avgF)]

    # Adesso ho bisogno dei top/bottom 5 per carbon e fce,
    # usiamo takeOrdered e top.
    #takeOrdered: RDD.takeOrdered(num: int, key: Optional[Callable[[T], S]] = None) → List[T]
    # num rappresenta quanti ne vogliamo; key è la chiave su cui ordiniamo

    #top: RDD.top(num: int, key: Optional[Callable[[T], S]] = None) → List[T]
    # Stessi parametri

    rdd_bottom_5_carbonDirect = result_rdd.takeOrdered(5, key=lambda x: x[2])
    rdd_bottom_5_fce = result_rdd.takeOrdered(5, key=lambda x: x[3])
    rdd_top_5_carbonDirect = result_rdd.top(5, key=lambda x: x[2])
    rdd_top_5_fce = result_rdd.top(5, key=lambda x: x[3])

    all_results_list = rdd_bottom_5_carbonDirect + rdd_top_5_carbonDirect + rdd_bottom_5_fce + rdd_top_5_fce
    all_results_rdd  = spark.sparkContext.parallelize(all_results_list)
    query_time = time.time() - query_initial_time
    # 2) Definisci l’header CSV
    header = (
    "record_year,record_month,avg_carbon_intensity,avg_cfe_percentage"
    )

    # 3) Trasforma le tuple in linee CSV
    csv_rdd = all_results_rdd.map(lambda row: ",".join(map(str, row)))

    # 4) Prepara l'RDD finale con header + dati, coalesciando in 1 partizione
    final_rdd = (
        spark.sparkContext
             .parallelize([header])
             .union(csv_rdd)
             .coalesce(1)
    )

    # 5) Scrivi su HDFS
    start_write = time.time()
    final_rdd.saveAsTextFile(HDFS_BASE_RESULT_PATH_Q2_RDD)
    write_time = time.time() - start_write
    logger.info(f"Tempo scrittura risultati: {write_time:.4f}s")

    total_time = read_time + query_time + write_time
    save_execution_time(QUERY_2_RDD, workers_number, read_time, query_time, write_time, total_time)

    spark.stop()

if __name__ == "__main__":
    workers_number = int(sys.argv[1])
    main(workers_number)

