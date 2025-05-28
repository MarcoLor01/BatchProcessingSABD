import sys
import time
import logging
from config import HDFS_PARQUET_PATH, HDFS_BASE_RESULT_PATH_Q2_RDD, QUERY_2_RDD
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from commonFunction import create_spark_session, save_execution_time
from pyspark.sql import Row

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


def get_top_n_rdd(rdd, key_func, n=5, ascending=True):
    return (rdd.sortBy(key_func, ascending=ascending)
            .zipWithIndex()
            .filter(lambda x: x[1] < n)
            .map(lambda x: x[0]))


def extract_year_month_and_transform(row):
    """Estrae anno/mese e trasforma direttamente in formato per aggregazione"""
    try:
        event_time = row.event_time

        record_year = int(event_time[:4])
        record_month = int(event_time[5:7])

        # Ritorna direttamente la chiave e i valori per l'aggregazione
        return (record_year, record_month), (row.CarbonDirect, row.CFEpercent)

    except (ValueError, TypeError, AttributeError):
        return None


def main(workers_number: int):
    spark = create_spark_session("Q2 Energy Stats RDD Optimized")

    start_time = time.time()

    # 1) Lettura e preprocessing più efficiente
    df = spark.read.schema(schema).parquet(HDFS_PARQUET_PATH)

    # Filtraggio e trasformazione in un solo passaggio ottimizzato
    italy_rdd = (df
                 .filter(df.Country == 'IT')  # Filtro anticipato
                 .rdd
                 .map(extract_year_month_and_transform)
                 .filter(lambda x: x is not None))

    # 2) Aggregazione ottimizzata con combineByKey
    def create_combiner(values):
        """Crea il combinatore iniziale da una tupla (carbon, cfe)"""
        return values[0], values[1], 1

    def merge_value(acc, values):
        """Merge di nuovi valori con l'accumulatore"""
        return acc[0] + values[0], acc[1] + values[1], acc[2] + 1

    def merge_combiners(acc1, acc2):
        """Merge di due accumulatori"""
        return acc1[0] + acc2[0], acc1[1] + acc2[1], acc1[2] + acc2[2]

    # Aggregazione diretta senza mapping aggiuntivo
    aggregated = italy_rdd.combineByKey(create_combiner, merge_value, merge_combiners)

    # 3) Calcolo delle medie e cache del risultato
    result_rdd = (aggregated
                  .map(lambda kv: (
        kv[0][0],  # Anno
        kv[0][1],  # Mese
        kv[1][0] / kv[1][2],  # Media CarbonDirect
        kv[1][1] / kv[1][2]  # Media CFEpercent
    ))
                  .cache())

    # takeOrdered: RDD.takeOrdered(num: int, key: Optional[Callable[[T], S]] = None) → List[T]
    # num rappresenta quanti ne vogliamo; key è la chiave su cui ordiniamo

    # top: RDD.top(num: int, key: Optional[Callable[[T], S]] = None) → List[T]
    # Stessi parametri
    rdd_bottom_5_carbonDirect = get_top_n_rdd(result_rdd, lambda x: x[2], ascending=True)
    rdd_top_5_carbonDirect = get_top_n_rdd(result_rdd, lambda x: x[2], ascending=False)
    rdd_bottom_5_fce = get_top_n_rdd(result_rdd, lambda x: x[3], ascending=True)
    rdd_top_5_fce = get_top_n_rdd(result_rdd, lambda x: x[3], ascending=False)

    final_time = time.time() - start_time

    # Combinazione dei risultati
    all_results_rdd = (rdd_top_5_carbonDirect
                       .union(rdd_bottom_5_carbonDirect)
                       .union(rdd_top_5_fce)
                       .union(rdd_bottom_5_fce))
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
    final_rdd.saveAsTextFile(HDFS_BASE_RESULT_PATH_Q2_RDD)
    save_execution_time(QUERY_2_RDD, workers_number, final_time)

    spark.stop()


if __name__ == "__main__":
    workers_number = int(sys.argv[1])
    main(workers_number)
