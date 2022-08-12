from pathlib import Path
import traceback
from kubernetes import client, config, utils


def try_create_from_yaml(api_client, yaml_path_str):
    try:
        utils.create_from_yaml(api_client, yaml_path_str)
    except Exception:
        traceback.print_exc()


if __name__ == "__main__":

    # config.load_config()
    # v1 = client.CoreV1Api()
    # print("Listing pods:")
    # ret = v1.list_pod_for_all_namespaces(watch=False)
    # for i in ret.items:
    #     print("%s\t%s\t%s" % (i.status.pod_ip, i.metadata.namespace, i.metadata.name))

    config.load_config()
    # config.load_incluster_config()
    yaml_folder_path = Path(__file__).absolute().parent
    api_client = client.ApiClient()

    # try_create_from_yaml(api_client, str(yaml_folder_path / "default_admin.yaml"))
    # try_create_from_yaml(api_client, str(yaml_folder_path / "nginx_pod.yaml"))

    v1 = client.CoreV1Api()
    nginx_pod = v1.read_namespaced_pod("nginx-pod", "default")
    nginx_pod.metadata.labels = {"name-label": "nginx-label"}
    v1.patch_namespaced_pod("nginx-pod", "default", nginx_pod)

    # try_create_from_yaml(api_client, str(yaml_folder_path / "nginx_service.yaml"))