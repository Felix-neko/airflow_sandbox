docker build . -t mlops-docker-snapshots.binary.alfabank.ru/airflow_fs_etl:v0.6.7 --network host --no-cache
docker push mlops-docker-snapshots.binary.alfabank.ru/airflow_fs_etl:v0.6.7