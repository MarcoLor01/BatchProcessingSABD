docker run --rm \
    --network <nome_tuo_compose_project>_data_network \
    -v ./your_script.py:/app/your_script.py \
    -v ./hadoop_conf:/opt/bitnami/spark/conf/hadoop_conf_custom \ # Se non copiato nel Dockerfile
    my-bitnami-spark-client