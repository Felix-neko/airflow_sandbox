from kubernetes import client, config
from kubernetes.config.kube_config import Configuration

if __name__ == "__main__":
    # config.load_kube_config(config_file="/home/felix/.kube/config", context="minikube")
    config.load_incluster_config()
    conf = Configuration()
    print(conf.host)