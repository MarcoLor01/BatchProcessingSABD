#!/bin/bash

NIFI_HOST=localhost
NIFI_PORT=8080
HDFS_INTERVAL=10
NIFI_INTERVAL=60
NIFI_API_BASE_URL="http://${NIFI_HOST}:${NIFI_PORT}/nifi-api"
NUM_RUNS=1
docker exec namenode hdfs dfsadmin -safemode leave
#Crea cartelle!!
echo "Svuoto cartelle risultati"
docker exec namenode hdfs dfs -rm -r /electricity_data_Q1_results/*
docker exec namenode hdfs dfs -rm -r /electricity_data_Q2_results/*
docker exec namenode hdfs dfs -rm -r /electricity_data_csv_total/*

set -euo pipefail

TARGET="./benchmark"

echo "Svuoto $TARGET …"
find "$TARGET" -mindepth 1 -exec rm -rf {} +
echo "Fatto."
# Funzione per controllare e installare pacchetti Python

install_package() {
  PACKAGE=$1
  if ! command -v "${PACKAGE}" &> /dev/null; then
  echo "${PACKAGE} non è installato. Lo installo..."
  sudo apt-get update && sudo apt-get install -y "${PACKAGE}"
else
  echo "${PACKAGE} è già installato."
fi
}

install_python_apt_package() {
  PACKAGE=$1
  PYPACKAGE="python3-${PACKAGE}"

  # Verifica se il pacchetto è già installato
  if dpkg -l | grep -q $PYPACKAGE; then
    echo "$PYPACKAGE è già installato."
  else
    echo "$PYPACKAGE non è installato. Lo installo..."
    sudo apt install -y $PYPACKAGE
  fi
}

wait_for_nifi() {
  echo "Aspettando che NiFi sia pronto..."
  MAX_RETRIES=60
  ATTEMPTS=0

  until curl -sf "${NIFI_API_BASE_URL}/system-diagnostics" > /dev/null; do
    ATTEMPTS=$((ATTEMPTS + 1))
    if [ "$ATTEMPTS" -ge "$MAX_RETRIES" ]; then
      echo "Timeout: NiFi non è pronto dopo $((MAX_RETRIES * NIFI_INTERVAL)) secondi."
      docker logs --tail 50 nifi
      exit 1
    fi
    echo "NiFi non ancora pronto, attendo ${NIFI_INTERVAL}s..."
    sleep ${NIFI_INTERVAL}
  done
  echo "NiFi è pronto."
}

start_root_pg() {
  echo "Avvio del Process Group Root..."

  ROOT_PG_JSON=$(curl -sf "${NIFI_API_BASE_URL}/flow/process-groups/root")
  ROOT_PG_ID=$(echo "$ROOT_PG_JSON" | jq -r '.processGroupFlow.id')

  curl -X PUT -H "Content-Type: application/json" \
       -d "{\"id\":\"${ROOT_PG_ID}\",\"state\":\"RUNNING\"}" \
       "${NIFI_API_BASE_URL}/flow/process-groups/${ROOT_PG_ID}"

  echo "Process Group Root avviato (ID: $ROOT_PG_ID)."
}

wait_for_hdfs_data() {
  echo "Attendo che HDFS contenga dati nella cartella electricity_data_parquet..."
  MAX_RETRIES=30
  RETRIES=0

  while true; do
    FILE_COUNT=$(docker exec namenode hdfs dfs -ls /electricity_data_parquet 2>/dev/null | grep -v '^Found' | wc -l)

    if [ "$FILE_COUNT" -gt 0 ]; then
      echo "Dati trovati in HDFS."
      break
    fi

    RETRIES=$((RETRIES + 1))
    if [ "$RETRIES" -ge "$MAX_RETRIES" ]; then
      echo "Timeout: Nessun dato trovato in HDFS dopo $((MAX_RETRIES * HDFS_INTERVAL)) secondi."
      exit 1
    fi

    echo "Nessun dato ancora in HDFS, ritento tra ${HDFS_INTERVAL} s..."
    sleep ${HDFS_INTERVAL}
  done
}

install_package jq
echo "1. Avvio dei servizi Docker Compose..."
#docker-compose up --build -d --scale spark-worker=1
#if [ $? -ne 0 ]; then
#  echo "Errore durante l'avvio di Docker Compose."
#  exit 1
#fi

echo "2. Attesa che NiFi sia disponibile..."
wait_for_nifi

echo "3. Avvio del flusso Root in NiFi..."
start_root_pg

echo "4. Attesa che i dati siano presenti in HDFS..."
wait_for_hdfs_data

#echo "5. Esecuzione della prima query..."
#for run in $(seq 1 $NUM_RUNS); do
#  echo "=== Run #$run ==="
#  docker exec da-spark-master spark-submit --deploy-mode client ./scripts/query1.py
#  if [ $? -ne 0 ]; then
#    echo "Errore durante l'esecuzione della prima Query."
#    exit 1
#  fi
#done


echo "5. Esecuzione della prima query SQL..."
for run in $(seq 1 $NUM_RUNS); do
  echo "=== Run #$run ==="
  docker exec da-spark-master spark-submit --deploy-mode client ./scripts/query1SQL.py
  if [ $? -ne 0 ]; then
    echo "Errore durante l'esecuzione della prima Query SQL."
    exit 1
  fi
done


#echo "6. Esecuzione dello script Spark 2..."
#for run in $(seq 1 $NUM_RUNS); do
#  echo "=== Run #$run ==="
#  docker exec da-spark-master spark-submit --deploy-mode client ./scripts/query2.py
#  if [ $? -ne 0 ]; then
#    echo "Errore durante l'esecuzione della seconda Query."
#    exit 1
#  fi
#done

echo "5. Esecuzione della seconda query SQL..."
for run in $(seq 1 $NUM_RUNS); do
  echo "=== Run #$run ==="
  docker exec da-spark-master spark-submit --deploy-mode client ./scripts/query2SQL.py
  if [ $? -ne 0 ]; then
    echo "Errore durante l'esecuzione della seconda Query SQL."
    exit 1
  fi
done

#echo "7. Esecuzione dello script Spark 3..."
#for run in $(seq 1 $NUM_RUNS); do
#  echo "=== Run #$run ==="
#  docker exec da-spark-master spark-submit --deploy-mode client ./scripts/query3.py
#  if [ $? -ne 0 ]; then
#    echo "Errore durante l'esecuzione della terza Query."
#    exit 1
#  fi
#done
#
#echo "8. Esecuzione dello script Spark 3 exact..."
#for run in $(seq 1 $NUM_RUNS); do
#  echo "=== Run #$run ==="
#  docker exec da-spark-master spark-submit --deploy-mode client ./scripts/query3exact.py
#  if [ $? -ne 0 ]; then
#    echo "Errore durante l'esecuzione della terza Query exact."
#    exit 1
#  fi
#done

echo "5. Esecuzione della terza query SQL..."
for run in $(seq 1 $NUM_RUNS); do
  echo "=== Run #$run ==="
  docker exec da-spark-master spark-submit --deploy-mode client ./scripts/query3SQL.py
  if [ $? -ne 0 ]; then
    echo "Errore durante l'esecuzione della terza Query SQL."
    exit 1
  fi
done

echo "6. Pulizia dei dati in HDFS..."
docker exec namenode hdfs dfs -rm -r /electricity_data/*
echo "Script completato con successo!"
exit 0
