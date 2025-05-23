import csv
import os
from config import RESULTS_CSV
from pyspark.sql import SparkSession


def save_execution_time(query_number, workers_number, tempo_lettura, tempo_query, tempo_scrittura, tempo_totale,
                        filename=RESULTS_CSV):
    print("Scrivo dati su CSV")
    file_esiste = os.path.isfile(filename)

    with open(filename, mode='a', newline='') as file:
        writer = csv.writer(file)

        if not file_esiste:
            print("Prima scrittura, aggiungo intestazione")
            writer.writerow(["query", "workers_number", "tempo_lettura", "tempo_query", "tempo_scrittura"])

        # Scrive una nuova riga con data/ora e i tre tempi
        writer.writerow([
            query_number,
            workers_number,
            round(tempo_lettura, 6),
            round(tempo_query, 6),
            round(tempo_scrittura, 6),
            round(tempo_totale, 6),
        ])
    print("Dati scritti")


def create_spark_session(app_name):
    spark = (SparkSession.builder
             .appName(app_name)
             .config("spark.sql.files.maxPartitionBytes", "128MB")
             .config("spark.sql.parquet.filterPushdown", "true")
             .config("spark.sql.parquet.enableVectorizedReader", "true")
             .config("spark.sql.adaptive.enabled", "true")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark
