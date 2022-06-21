BASEDIR=$(dirname "$0")
cd $BASEDIR

#helm uninstall airflow
#kubectl delete -f host_dags_pv.yaml
#kubectl delete -f host_dags_pvc.yaml

kubectl apply -f host_dags_pv.yaml
kubectl apply -f host_dags_pvc.yaml

helm upgrade --install airflow apache-airflow/airflow --set dags.persistence.enabled=true --set dags.persistence.existingClaim=host-dags-pvc --set dags.gitSync.enabled=false --set executor=KubernetesExecutor --set logs.persistence.enabled=true --set image.tag=latest-python3.7