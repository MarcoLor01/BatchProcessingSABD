import sys
import time
import logging
from config import HDFS_PARQUET_PATH, HDFS_BASE_RESULT_PATH_Q3_RDD, QUERY_3_RDD
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from commonFunction import create_spark_session, save_execution_time, write_rdd_hdfs, format_row_6_decimals

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
        return 0.0, 0.0, 0.0, 0.0, 0.0

    sorted_values = sorted(values)

    return (
        sorted_values[0],  # min
        calculate_percentile(sorted_values, 0.25),  # 25%
        calculate_percentile(sorted_values, 0.50),  # 50%
        calculate_percentile(sorted_values, 0.75),  # 75%
        sorted_values[-1]  # max
    )


def extract_hour_and_transform(row):
    """Estrae ora e trasforma direttamente in formato per aggregazione"""
    try:
        # Estrai l'ora dai caratteri 11 a 13 (posizioni della stringa per "HH")
        record_hour = int(row.event_time[11:13])

        # Ritorna chiave e valori per l'aggregazione
        return (row.Country, record_hour), (row.CarbonDirect, row.CFEpercent, 1)

    except (ValueError, TypeError):
        return None


def main(workers_number: int):
    spark = create_spark_session("Q3 Energy Stats RDD", "RDD", workers_number)

    # 1) Lettura dati Parquet e trasformazione diretta
    start_read = time.time()

    # Leggi e trasforma direttamente nel formato per aggregazione
    base_rdd = (
        spark.read.schema(schema)
        .parquet(HDFS_PARQUET_PATH)
        .rdd
        .map(extract_hour_and_transform)
        .filter(lambda x: x is not None)
    )

    # 2) Calcolo somme e conteggi per (Country, Hour)
    hourly_agg = base_rdd.aggregateByKey(
        (0.0, 0.0, 0),  # (sum_carbon, sum_cfe, count)
        lambda acc, v: (acc[0] + v[0], acc[1] + v[1], acc[2] + v[2]),
        lambda a, b: (a[0] + b[0], a[1] + b[1], a[2] + b[2])
    )

    # 3) Funzioni per aggregazione finale per paese
    def seq_op_stats(acc, val):
        """Combina un nuovo valore con l'accumulatore"""
        carbon_vals, cfe_vals = acc
        carbon_avg, cfe_avg = val
        return carbon_vals + [carbon_avg], cfe_vals + [cfe_avg]

    def comb_op_stats(acc1, acc2):
        """Combina due accumulatori"""
        return acc1[0] + acc2[0], acc1[1] + acc2[1]

    # 4) Aggregazione per paese con calcolo delle medie orarie
    country_stats = (
        hourly_agg
        .map(lambda kv: (kv[0][0], (kv[1][0] / kv[1][2], kv[1][1] / kv[1][2])))  # (country, (avg_carbon, avg_cfe))
        .aggregateByKey(
            ([], []),  # Valore iniziale: (lista_carbon, lista_cfe)
            seq_op_stats,
            comb_op_stats
        )
        .map(lambda kv: (
            kv[0],  # country
            calculate_stats(kv[1][0]),  # Stats per carbon
            calculate_stats(kv[1][1])  # Stats per CFE
        ))
    )

    # 5) Trasforma in formato finale - lista di tuple per ogni riga del CSV
    results = country_stats.flatMap(lambda rec: [
        (rec[0], 'carbon-intensity', *rec[1]),
        (rec[0], 'cfe', *rec[2])
    ])

    final_time = time.time() - start_read

    header = "country,data,min,25-perc,50-perc,75-perc,max"
    write_rdd_hdfs(spark, header, results, HDFS_BASE_RESULT_PATH_Q3_RDD, format_row_6_decimals)
    save_execution_time(QUERY_3_RDD, workers_number, final_time)
    spark.stop()


if __name__ == "__main__":
    workers_number = int(sys.argv[1])
    main(workers_number)
