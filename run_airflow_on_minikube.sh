BASEDIR=$(dirname "$0")
cd $BASEDIR

helm uninstall airflow
kubectl delete -f host_dags_pvc.yaml
kubectl delete -f host_dags_pv.yaml


kubectl apply -f host_dags_pv.yaml
kubectl apply -f host_dags_pvc.yaml

helm repo add airflow-stable https://airflow-helm.github.io/charts
helm upgrade airflow airflow-stable/airflow --install -f values.yaml