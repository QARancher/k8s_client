import yaml
from kubernetes import client, config

from k8s_client.pod import PodClient
from k8s_client.node import NodeClient
from k8s_client.secret import SecretClient
from k8s_client.service import ServiceClient
from k8s_client.namespace import NamespaceClient
from k8s_client.daemonset import DaemonSetClient
from k8s_client.deployment import DeploymentClient
from k8s_client.exceptions import K8sInvalidResourceBody
from k8s_client.consts import (DEFAULT_MAX_THREADS, KUBECONFIG_PATH)


class K8sClient(object):

    def __init__(self, kubeconfig_path=KUBECONFIG_PATH):
        # Configure the client to the k8s environment
        config.load_kube_config(kubeconfig_path)
        configuration = client.Configuration()
        configuration.assert_hostname = False
        client.Configuration.set_default(configuration)
        client_core = client.CoreV1Api()
        client_app = client.AppsV1Api()

        # Create the instances of the resources
        self.pod = PodClient(client_core=client_core)
        self.deployment = DeploymentClient(client_app=client_app, pod=self.pod)
        self.daemon_set = DaemonSetClient(client_app=client_app,
                                          deployment=self.deployment,
                                          pod=self.pod)
        self.namespace = NamespaceClient(client_core=client_core)
        self.node = NodeClient(client_core=client_core)
        self.secret = SecretClient(client_core=client_core)
        self.service = ServiceClient(client_core=client_core)

    def create_from_yaml(self, yaml_path, wait=True,
                         max_threads=DEFAULT_MAX_THREADS):
        with open(yaml_path, "r") as f:
            for resource in yaml.load_all(f.read()):
                if resource.get("kind") is None:
                    raise K8sInvalidResourceBody()

                if resource["kind"] == "Pod":
                    self.pod.create(body=resource, wait=wait)
                if resource["kind"] == "Deployment":
                    self.deployment.create(body=resource, wait=wait,
                                           max_threads=max_threads)
                if resource["kind"] == "DaemonSet":
                    self.daemon_set.create(body=resource, wait=wait,
                                           max_threads=max_threads)
                if resource["kind"] == "Namespace":
                    self.namespace.create(body=resource, wait=wait)
                if resource["kind"] == "Secret":
                    self.pod.create(body=resource, wait=wait)
                if resource["kind"] == "Service":
                    self.service.create(body=resource, wait=wait)
                else:
                    raise K8sInvalidResourceBody("unsupported resource type")


if __name__ == "__main__":
    pass
