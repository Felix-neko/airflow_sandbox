BASEDIR=$(dirname "$0")

kubectl delete -f $BASEDIR/nginx_service.yaml -f $BASEDIR/nginx_pod.yaml -f $BASEDIR/default_admin.yaml