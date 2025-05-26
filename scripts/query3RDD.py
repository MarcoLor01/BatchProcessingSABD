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

    # Mappa per creare coppie ((Country, Hour), valori)
    pairs = rdd.map(lambda row: (
        (row.Country, row.record_hour),
        (row.CarbonDirect, row.CFEpercent, 1)
    ))

    # Funzioni per aggregateByKey
    zero = (0.0, 0.0, 0)

    def seq_op(acc, val):
        sumC, sumF, cnt = acc
        c, f, _ = val
        return (sumC + c, sumF + f, cnt + 1)

    def comb_op(a, b):
        return (a[0] + b[0], a[1] + b[1], a[2] + b[2])

    # Calcola medie orarie per (paese, ora)
    hourly_aggregated = pairs.aggregateByKey(zero, seq_op, comb_op)

    # Calcola le medie e crea RDD (Country, (AvgCarbon, AvgCFE))
    hourly_averages = hourly_aggregated.map(lambda kv: (
        kv[0][0],  # Country
        (kv[1][0] / kv[1][2], kv[1][1] / kv[1][2])  # (AvgCarbon, AvgCFE)
    ))

    # Raggruppa per paese usando groupByKey (appropriato perché abbiamo solo 24 ore per paese)
    country_groups = hourly_averages.groupByKey()

    # Calcola le statistiche per ciascun paese
    country_stats = country_groups.map(lambda kv: {
        'country': kv[0],
        'hourly_values': list(kv[1])  # Converti l'iterabile in lista
    }).map(lambda data: {
        'country': data['country'],
        'carbon_values': [val[0] for val in data['hourly_values']],
        'cfe_values': [val[1] for val in data['hourly_values']]
    }).map(lambda data: {
        'country': data['country'],
        'carbon_stats': calculate_stats(data['carbon_values']),
        'cfe_stats': calculate_stats(data['cfe_values'])
    })

    # Trasforma in formato finale - lista di tuple per ogni riga del CSV
    results = country_stats.flatMap(lambda stats: [
        (
            stats['country'],
            'carbon-intensity',
            stats['carbon_stats'][0],  # min
            stats['carbon_stats'][1],  # 25%
            stats['carbon_stats'][2],  # 50%
            stats['carbon_stats'][3],  # 75%
            stats['carbon_stats'][4]   # max
        ),
        (
            stats['country'],
            'cfe',
            stats['cfe_stats'][0],  # min
            stats['cfe_stats'][1],  # 25%
            stats['cfe_stats'][2],  # 50%
            stats['cfe_stats'][3],  # 75%
            stats['cfe_stats'][4]   # max
        )
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