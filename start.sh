#!/bin/bash

NIFI_HOST=localhost
NIFI_PORT=8080
HDFS_INTERVAL=10
NIFI_INTERVAL=60
NIFI_API_BASE_URL="http://${NIFI_HOST}:${NIFI_PORT}/nifi-api"
NUM_RUNS=1
TARGET="./benchmark"
TO_REDIS="374c5e9d-0197-1000-a538-bc3fa9191f9f"
TO_HDFS="374cbdb9-0197-1000-e279-a9653bdd6b13"
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

start_to_hdfs_pg() {
  echo "=== Avvio del Process Group TO_HDFS ==="
  curl -X PUT -H "Content-Type: application/json" \
       -d "{\"id\":\"${TO_HDFS}\",\"state\":\"RUNNING\"}" \
       "${NIFI_API_BASE_URL}/flow/process-groups/${TO_HDFS}" \
       && echo "• Process Group TO_HDFS avviato (ID: ${TO_HDFS})."
}

stop_to_hdfs_pg() {
  echo "=== Arresto del Process Group TO_HDFS ==="
  
  sleep 5
  curl -X PUT -H "Content-Type: application/json" \
       -d "{\"id\":\"${TO_HDFS}\",\"state\":\"STOPPED\"}" \
       "${NIFI_API_BASE_URL}/flow/process-groups/${TO_HDFS}" \
       && echo "• Process Group TO_HDFS fermato (ID: ${TO_HDFS})."
}

start_to_redis_pg() {
  echo "Avvio del Process Group TO_REDIS (ID: $TO_REDIS)..."
  curl -X PUT -H "Content-Type: application/json" \
       -d "{\"id\":\"${TO_REDIS}\",\"state\":\"RUNNING\"}" \
       "${NIFI_API_BASE_URL}/flow/process-groups/${TO_REDIS}"
  echo "Process Group TO_REDIS avviato (ID: $TO_REDIS)."
}

stop_to_redis_pg() {
  echo "Arresto del Process Group TO_REDIS (ID: $TO_REDIS)..."
  sleep 15
  curl -X PUT -H "Content-Type: application/json" \
       -d "{\"id\":\"${TO_REDIS}\",\"state\":\"STOPPED\"}" \
       "${NIFI_API_BASE_URL}/flow/process-groups/${TO_REDIS}"
  echo "Process Group TO_REDIS fermato (ID: $TO_REDIS)."
}


wait_for_hdfs_data() {
  echo "Attendo che HDFS contenga almeno 2 file in /electricity_data_parquet..."
  MAX_RETRIES=30
  RETRIES=0

  # Primo controllo: almeno 2 file in /electricity_data_parquet
  while true; do
    FILE_COUNT=$(docker exec namenode hdfs dfs -ls /user/root/electricity_data_parquet 2>/dev/null | grep -v '^Found' | wc -l)
    if [ "$FILE_COUNT" -ge 2 ]; then
      echo "Trovati 2 file (o più) in /electricity_data_parquet."
      break
    fi

    RETRIES=$((RETRIES + 1))
    if [ "$RETRIES" -ge "$MAX_RETRIES" ]; then
      echo "Timeout: meno di 2 file trovati in /electricity_data_parquet dopo $((MAX_RETRIES * HDFS_INTERVAL)) secondi."
      exit 1
    fi

    echo "File trovati in /electricity_data_parquet: $FILE_COUNT. Ritento tra ${HDFS_INTERVAL}s..."
    sleep "${HDFS_INTERVAL}"
  done

  echo "Attendo che HDFS contenga almeno 2 file in /electricity_data_csv..."
  RETRIES=0

  # Secondo controllo: almeno 2 file in /electricity_data_csv
  while true; do
    FILE_COUNT=$(docker exec namenode hdfs dfs -ls /user/root/electricity_data_csv 2>/dev/null | grep -v '^Found' | wc -l)
    if [ "$FILE_COUNT" -ge 2 ]; then
      echo "Trovati 2 file (o più) in /electricity_data_csv."
      break
    fi

    RETRIES=$((RETRIES + 1))
    if [ "$RETRIES" -ge "$MAX_RETRIES" ]; then
      echo "Timeout: meno di 2 file trovati in /electricity_data_csv dopo $((MAX_RETRIES * HDFS_INTERVAL)) secondi."
      exit 1
    fi

    echo "File trovati in /electricity_data_csv: $FILE_COUNT. Ritento tra ${HDFS_INTERVAL}s..."
    sleep "${HDFS_INTERVAL}"
  done

  echo "Entrambi i controlli superati: presenti almeno 2 file in /electricity_data_parquet e 2 file in /electricity_data_csv. Continuo..."
}

###################### INIZIO ESECUZIONE #########################

install_package jq

echo "Svuoto cartella risultati benchmark $TARGET …"
  find "$TARGET" -mindepth 1 -exec rm -rf {} +
  echo "Fatto."

  for WORKERS in $(seq 1 1); do
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

  docker exec namenode hdfs dfs -rm -r /user/root/electricity_data_parquet/*
  docker exec namenode hdfs dfs -rm -r /user/root/electricity_data_csv/*

  echo "3. Avvio del flusso HDFS in NiFi..."
  start_to_hdfs_pg

  echo "4. Attesa che i dati siano presenti in HDFS..."
  wait_for_hdfs_data

  echo "3. Stop del flusso Root in NiFi..."
  stop_to_hdfs_pg &

  for run in $(seq 1 $NUM_RUNS); do
    echo "=== Run #$run con ${WORKERS} worker(s) ==="

    echo "Svuoto directory risultati..."
    docker exec namenode hdfs dfs -rm -r /user/root/result/query1/*
    docker exec namenode hdfs dfs -rm -r /user/root/result/query2/*
    docker exec namenode hdfs dfs -rm -r /user/root/result/query3/*
    docker exec namenode hdfs dfs -rm -r /user/root/result/query1rdd/*
    docker exec namenode hdfs dfs -rm -r /user/root/result/query2rdd/*
    docker exec namenode hdfs dfs -rm -r /user/root/result/query3rdd/*
    docker exec namenode hdfs dfs -rm -r /user/root/result/query1sql/*
    docker exec namenode hdfs dfs -rm -r /user/root/result/query2sql/*
    docker exec namenode hdfs dfs -rm -r /user/root/result/query3sql/*
    docker exec namenode hdfs dfs -rm -r /user/root/result/query3exact/*
    echo "Completato, inizio esecuzioni query"

    echo "Esecuzione query2SQL.py..."
    #docker exec da-spark-master spark-submit --deploy-mode client /opt/spark/src/queries/query2SQL.py "parquet" $WORKERS || exit 1
    #docker exec da-spark-master spark-submit --deploy-mode client /opt/spark/src/queries/query2SQL.py "csv" $WORKERS || exit 1

    echo "Esecuzione query1RDD.py..."
    docker exec da-spark-master spark-submit --deploy-mode client /opt/spark/src/queries/query1RDD.py $WORKERS || exit 1

    echo "Esecuzione query2RDD.py..."
    docker exec da-spark-master spark-submit --deploy-mode client /opt/spark/src/queries/query2RDD.py $WORKERS || exit 1

    echo "Esecuzione query3RDD.py..."
    docker exec da-spark-master spark-submit --deploy-mode client /opt/spark/src/queries/query3RDD.py $WORKERS || exit 1

    echo "Esecuzione query1.py..."
    #docker exec da-spark-master spark-submit --deploy-mode client /opt/spark/src/queries/query1.py "parquet" $WORKERS || exit 1
    #docker exec da-spark-master spark-submit --deploy-mode client /opt/spark/src/queries/query1.py "csv" $WORKERS || exit 1

    echo "Esecuzione query1SQL.py..."
    #docker exec da-spark-master spark-submit --deploy-mode client /opt/spark/src/queries/query1SQL.py "parquet" $WORKERS || exit 1
    #docker exec da-spark-master spark-submit --deploy-mode client /opt/spark/src/queries/query1SQL.py "csv" $WORKERS || exit 1

    echo "Esecuzione query2.py..."
    #docker exec da-spark-master spark-submit --deploy-mode client /opt/spark/src/queries/query2.py "parquet" $WORKERS || exit 1
    #docker exec da-spark-master spark-submit --deploy-mode client /opt/spark/src/queries/query2.py "csv" $WORKERS || exit 1

    echo "Esecuzione query3.py..."
    #docker exec da-spark-master spark-submit --deploy-mode client /opt/spark/src/queries/query3.py "parquet" $WORKERS || exit 1
    #docker exec da-spark-master spark-submit --deploy-mode client /opt/spark/src/queries/query3.py "csv" $WORKERS || exit 1

    echo "Esecuzione query3exact.py..."
    #docker exec da-spark-master spark-submit --deploy-mode client /opt/spark/src/queries/query3exact.py "parquet" $WORKERS || exit 1
    #docker exec da-spark-master spark-submit --deploy-mode client /opt/spark/src/queries/query3exact.py "csv" $WORKERS || exit 1

    echo "Esecuzione query3SQL.py..."
    #docker exec da-spark-master spark-submit --deploy-mode client /opt/spark/src/queries/query3SQL.py "parquet" $WORKERS || exit 1
    #docker exec da-spark-master spark-submit --deploy-mode client /opt/spark/src/queries/query3SQL.py "csv" $WORKERS || exit 1
  done
done
    echo "Esporto risultati da HDFS a Redis tramite NiFi..."

    #docker exec redis redis-cli FLUSHALL
    start_to_redis_pg

    echo "Completato, stop del processo NiFi..."
    stop_to_redis_pg
    #docker-compose down

echo "Script completato"
