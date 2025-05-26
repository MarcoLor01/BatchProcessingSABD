import sys
import time
import logging
from config import HDFS_PARQUET_PATH, HDFS_BASE_RESULT_PATH_Q3_RDD, QUERY_3_RDD
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
    StructField("record_hour", IntegerType(), True),
    StructField("CarbonDirect", DoubleType(), True),
    StructField("CFEpercent", DoubleType(), True),
])


def calculate_percentile(sorted_data, p):
    """Calcola il percentile correttamente usando interpolazione lineare"""
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
    """Calcola tutte le statistiche richieste"""
    if not values:
        return (0.0, 0.0, 0.0, 0.0, 0.0)

    sorted_values = sorted(values)

    return (
        sorted_values[0],  # min
        calculate_percentile(sorted_values, 0.25),  # 25%
        calculate_percentile(sorted_values, 0.50),  # 50%
        calculate_percentile(sorted_values, 0.75),  # 75%
        sorted_values[-1]  # max
    )


# Facendo riferimento al dataset dei valori energetici dell'Italia e della Svezia, aggregare i dati di ciascun
# paese sulle 24 ore della giornata, calcolando il valor medio di "Carbon intensity gCO2eq/kWh
# (direct)" e "Carbon-free energy percentage (CFE%)". Calcolare il minimo, 25-esimo, 50-esimo, 75-
# esimo percentile e massimo del valor medio di "Carbon intensity gCO2eq/kWh (direct)" e "Carbon-free
# energy percentage (CFE%)".

def main(workers_number: int):
    spark = create_spark_session("Q3 Energy Stats RDD")

    # 1) Lettura dati Parquet
    start_read = time.time()
    df = spark.read.schema(schema).parquet(HDFS_PARQUET_PATH)
    rdd = df.rdd
    read_time = time.time() - start_read
    logger.info(f"Tempo lettura Parquet: {read_time:.10f}s")

    # 2) Inizio misurazione query
    start_query = time.time()

    # 3) Calcolo somme e conteggi per (Country, Hour)
    hourly_agg = (
        rdd
        .map(lambda row: ((row.Country, row.record_hour), (row.CarbonDirect, row.CFEpercent, 1)))
        .aggregateByKey(
            (0.0, 0.0, 0),
            lambda acc, v: (acc[0] + v[0], acc[1] + v[1], acc[2] + v[2]),
            lambda a, b: (a[0] + b[0], a[1] + b[1], a[2] + b[2])
        )
    )

    # 4) Media oraria e raggruppamento per paese
    country_stats = (
        hourly_agg
        .map(lambda kv: (kv[0][0], (kv[1][0] / kv[1][2], kv[1][1] / kv[1][2])))
        .groupByKey()
        .mapValues(lambda vals: list(vals))
        .map(lambda kv: (
            kv[0],
            calculate_stats([v[0] for v in kv[1]]),
            calculate_stats([v[1] for v in kv[1]])
        ))
    )

    # Trasforma in formato finale - lista di tuple per ogni riga del CSV
    results = country_stats.flatMap(lambda rec: [
        (rec[0], 'carbon-intensity', *rec[1]),
        (rec[0], 'cfe', *rec[2])
    ])

    query_time = time.time() - start_query
    logger.info(f"Tempo esecuzione query: {query_time:.10f}s")

    # Collezioniamo e stampiamo i risultati per debug
    collected_results = results.collect()
    for result in collected_results:
        country, metric, min_val, p25, p50, p75, max_val = result
        logger.info(f"{country} - {metric}: Min={min_val:.6f}, 25%={p25:.6f}, "
                    f"50%={p50:.6f}, 75%={p75:.6f}, Max={max_val:.6f}")

    # 2) Definisci l'header CSV
    header = "country,data,min,25-perc,50-perc,75-perc,max"

    # 3) Trasforma le tuple in linee CSV con formattazione a 6 decimali
    csv_rdd = results.map(lambda row: "{},{},{:.6f},{:.6f},{:.6f},{:.6f},{:.6f}".format(
        row[0], row[1], row[2], row[3], row[4], row[5], row[6]
    ))

    # 4) Prepara l'RDD finale con header + dati, coalesciando in 1 partizione
    final_rdd = (
        spark.sparkContext
        .parallelize([header])
        .union(csv_rdd)
        .coalesce(1)
    )

    # 5) Scrivi su HDFS
    start_write = time.time()
    final_rdd.saveAsTextFile(HDFS_BASE_RESULT_PATH_Q3_RDD)
    write_time = time.time() - start_write
    logger.info(f"Tempo scrittura risultati: {write_time:.4f}s")

    # Salva tempo di esecuzione totale
    total_time = read_time + query_time + write_time
    save_execution_time(QUERY_3_RDD, workers_number, read_time, query_time, write_time, total_time)

    spark.stop()

    return total_time


if __name__ == "__main__":
    workers_number = int(sys.argv[1])
    main(workers_number)
