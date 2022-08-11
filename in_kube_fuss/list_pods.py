from kubernetes import client, config


if __name__ == "__main__":
    config.load_config()
    v1 = client.CoreV1Api()

    nginx_pod = v1.read_namespaced_pod("nginx-pod", "default")
    nginx_pod.metadata.labels = {"label-name": "nginx-label"}
    v1.patch_namespaced_pod("nginx-pod", "default", nginx_pod)
    print(nginx_pod)





    # ret = v1.list_pod_for_all_namespaces(watch=False)
    # for i, pod in enumerate(ret.items):
    #     print(i, f"{pod.metadata.namespace}.{pod.metadata.name}")

    # nginx_pod = ret.items[0]
    # print(nginx_pod)