#!/bin/bash

NIFI_HOST=localhost
NIFI_PORT=8080
HDFS_INTERVAL=10
NIFI_INTERVAL=60
NIFI_API_BASE_URL="http://${NIFI_HOST}:${NIFI_PORT}/nifi-api"
NUM_RUNS=1
TARGET="./benchmark"

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

echo "Svuoto cartella risultati benchmark $TARGET …"
  find "$TARGET" -mindepth 1 -exec rm -rf {} +
  echo "Fatto."

for WORKERS in 1 2 3; do
  echo "=== ESECUZIONE CON ${WORKERS} WORKER(S) ==="

  echo "1. Avvio dei servizi Docker Compose con ${WORKERS} worker..."
  #docker-compose down
  #docker-compose up --build -d --scale spark-worker=$WORKERS
  if [ $? -ne 0 ]; then
    echo "Errore durante l'avvio di Docker Compose."
    exit 1
  fi

  echo "2. Attesa che NiFi sia disponibile..."
  wait_for_nifi

  echo "3. Avvio del flusso Root in NiFi..."
  start_root_pg

  echo "4. Attesa che i dati siano presenti in HDFS..."
  wait_for_hdfs_data

  for run in $(seq 1 $NUM_RUNS); do
    echo "=== Run #$run con ${WORKERS} worker(s) ==="

    echo "Esecuzione query1RDD.py..."
    docker exec da-spark-master spark-submit --deploy-mode client ./scripts/query1RDD.py $WORKERS || exit 1

    #echo "Esecuzione query1.py..."
    #docker exec da-spark-master spark-submit --deploy-mode client ./scripts/query1.py $WORKERS || exit 1

    echo "Esecuzione query1SQL.py..."
    #docker exec da-spark-master spark-submit --deploy-mode client ./scripts/query1SQL.py || exit 1

    echo "Esecuzione query2.py..."
    #docker exec da-spark-master spark-submit --deploy-mode client ./scripts/query2.py $WORKERS || exit 1

    echo "Esecuzione query2SQL.py..."
    #docker exec da-spark-master spark-submit --deploy-mode client ./scripts/query2SQL.py || exit 1

    echo "Esecuzione query3.py..."
    #docker exec da-spark-master spark-submit --deploy-mode client ./scripts/query3.py $WORKERS || exit 1

    echo "Esecuzione query3exact.py..."
    #docker exec da-spark-master spark-submit --deploy-mode client ./scripts/query3exact.py $WORKERS || exit 1

    echo "Esecuzione query3SQL.py..."
    #docker exec da-spark-master spark-submit --deploy-mode client ./scripts/query3SQL.py || exit 1
  done

  echo "Pulizia dati da HDFS..."
  docker exec namenode hdfs dfs -rm -r /electricity_data_parquet/*
  docker exec namenode hdfs dfs -rm -r /electricity_data_Q1_results/*
  docker exec namenode hdfs dfs -rm -r /electricity_data_Q2_results/*
  echo "Pulizia completata."

done

echo "Script completato con successo!"
