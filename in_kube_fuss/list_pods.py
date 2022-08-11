from kubernetes import client, config


if __name__ == "__main__":
    config.load_config()
    v1 = client.CoreV1Api()
    ret = v1.list_pod_for_all_namespaces(watch=False)
    for i, pod in enumerate(ret.items):
        print(i, f"{pod.metadata.namespace}.{pod.metadata.name}")