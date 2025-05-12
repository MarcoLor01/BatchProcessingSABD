import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, year, month, hour, expr
)

# Configurazione dell'ambiente
os.environ['PYSPARK_PYTHON'] = 'python3'
os.environ['HADOOP_CONF_DIR'] = '/opt/hadoop/etc/hadoop'

# Configurazione paths e parametri
hdfs_base_path = "hdfs://namenode:9000/electricity_data"
countries = ["IT", "SE"]
years = list(range(2021, 2024))

# Creazione SparkSession con configurazioni ottimizzate per HDFS
spark = (SparkSession.builder
         .appName("Energy Data Analysis")
         .master("spark://spark-master:7077")
         .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
         .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
         .getOrCreate())

try:
    print("Inizio elaborazione dei dati energetici")

    for country in countries:
        for file_year in years:
            file_path = f"{hdfs_base_path}/{country}_{file_year}_hourly.csv"
            try:
                print(f"Elaborazione: {file_path}")

                # Lettura e trasformazione del DataFrame
                df = (spark.read
                .option("header", True)
                .option("inferSchema", True)
                .csv(file_path)
                # rinomina
                .withColumnRenamed("Datetime (UTC)", "datetime_utc")
                .withColumnRenamed("Carbon intensity gCO₂eq/kWh (direct)", "carbon_intensity")
                .withColumnRenamed("Carbon-free energy percentage (CFE%)", "cfe_pct")
                # timestamp e derivati
                .withColumn("datetime_utc", to_timestamp("datetime_utc"))
                .withColumn("year", year("datetime_utc"))
                .withColumn("month", month("datetime_utc"))
                .withColumn("hour", hour("datetime_utc"))
                # cast numerici
                .withColumn("carbon_intensity", col("carbon_intensity").cast("double"))
                .withColumn("cfe_pct", col("cfe_pct").cast("double"))
                # country
                .withColumn("country", expr(f"'{country}'"))
                # drop colonne non necessarie
                .drop(
                    "Country",
                    "Zone name",
                    "Zone id",
                    "Carbon intensity gCO₂eq/kWh (Life cycle)",
                    "Renewable energy percentage (RE%)",
                    "Data source",
                    "Data estimated",
                    "Data estimation method"
                )
                )

                # Debug: mostra schema e primi record
                print(f"Schema per {country} {file_year}:")
                df.printSchema()

                print(f"Dati di esempio per {country} {file_year}:")
                df.show(5, truncate=False)

                # Esempio: salva il risultato su HDFS in formato parquet
                output_path = f"{hdfs_base_path}/processed/{country}/{file_year}"
                print(f"Salvataggio risultati in: {output_path}")
                df.write.mode("overwrite").parquet(output_path)

            except Exception as e:
                print(f"Errore durante l'elaborazione di {file_path}: {e}")

    print("Elaborazione completata con successo!")

except Exception as e:
    print(f"Errore generale: {e}")

finally:
    # Chiusura della sessione Spark
    if spark:
        spark.stop()
        print("Sessione Spark terminata")