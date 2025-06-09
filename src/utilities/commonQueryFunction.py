import csv
import os
from pyspark.sql import SparkSession

RESULTS_CSV = "/app/benchmark/execution_time.csv"


def save_execution_time(query_number, workers_number, tempo_totale,
                        filename=RESULTS_CSV):
    print("Scrivo dati su CSV")
    file_esiste = os.path.isfile(filename)

    with open(filename, mode='a', newline='') as file:
        writer = csv.writer(file)

        if not file_esiste:
            writer.writerow(["query", "workers_number", "tempo_totale"])

        # Scrive una nuova riga con data/ora e i tre tempi
        writer.writerow([
            query_number,
            workers_number,
            round(tempo_totale, 6),
        ])


def create_spark_session(app_name: str, mode: str, target_parallelism: int) -> SparkSession:
    builder = SparkSession.builder.appName(app_name)
    #builder = builder.config("spark.sql.files.maxPartitionBytes", "128MB")
    #builder = builder.config("spark.sql.parquet.filterPushdown", "true")
    #builder = builder.config("spark.sql.parquet.enableVectorizedReader", "true")
    #builder = builder.config("spark.sql.adaptive.enabled", "true")

    #if mode == "DF":
    #    builder = builder.config("spark.sql.shuffle.partitions", target_parallelism)
    #elif mode == "RDD":
    #    builder = builder.config("spark.default.parallelism", target_parallelism)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def write_rdd_hdfs(result_rdd, output_path, column_names):
    df = result_rdd.toDF(column_names)
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)
