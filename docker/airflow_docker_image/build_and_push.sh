docker build . -t mlops-docker-snapshots.binary.alfabank.ru/airflow_fs_etl:latest --network host --no-cache
docker push mlops-docker-snapshots.binary.alfabank.ru/airflow_fs_etl:latest