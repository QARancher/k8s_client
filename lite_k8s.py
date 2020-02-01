import yaml
from kubernetes import client, config


from lite_k8s_cli.pod import PodClient
from lite_k8s_cli.node import NodeClient
from commons.decorators import poll_timeout
from lite_k8s_cli.secret import SecretClient
from lite_k8s_cli.service import ServiceClient
from lite_k8s_cli.namespace import NamespaceClient
from lite_k8s_cli.daemonset import DaemonSetClient
from lite_k8s_cli.deployment import DeploymentClient
from lite_k8s_cli.exceptions import K8sInvalidResourceBody
from lite_k8s_cli.consts import (
    WAIT_TIMEOUT,
    DEFAULT_MAX_THREADS,
    AFW_KUBECONFIG_PATH
)


class K8sClient(object):

    def __init__(self,
                 afw_kubeconfig_path=AFW_KUBECONFIG_PATH):
        # Configure the client to the k8s environment
        config.load_kube_config(afw_kubeconfig_path)
        configuration = client.Configuration()
        configuration.assert_hostname = False
        client.Configuration.set_default(configuration)
        client_core = client.CoreV1Api()
        client_app = client.AppsV1Api()

        # Create the instances of the resources
        self.pod = PodClient(client_core=client_core)
        self.deployment = DeploymentClient(client_app=client_app,
                                           pod=self.pod)
        self.daemon_set = DaemonSetClient(client_app=client_app,
                                          deployment=self.deployment,
                                          pod=self.pod)
        self.namespace = NamespaceClient(client_core=client_core)
        self.node = NodeClient(client_core=client_core)
        self.secret = SecretClient(client_core=client_core)
        self.service = ServiceClient(client_core=client_core)

    @poll_timeout(default_timeout=WAIT_TIMEOUT)
    def create_from_yaml(self,
                         yaml_path,
                         wait=True,
                         timeout=WAIT_TIMEOUT,
                         max_threads=DEFAULT_MAX_THREADS):
        with open(yaml_path, "r") as f:
            for resource in yaml.load_all(f.read()):
                if resource.get("kind") is None:
                    raise K8sInvalidResourceBody()

                if resource["kind"] == "Pod":
                    self.pod.create(body=resource,
                                    wait=wait,
                                    timeout=timeout)
                if resource["kind"] == "Deployment":
                    self.deployment.create(body=resource,
                                           wait=wait,
                                           timeout=timeout,
                                           max_threads=max_threads)
                if resource["kind"] == "DaemonSet":
                    self.daemon_set.create(body=resource,
                                           wait=wait,
                                           timeout=timeout,
                                           max_threads=max_threads)
                if resource["kind"] == "Namespace":
                    self.namespace.create(body=resource,
                                          wait=wait,
                                          timeout=timeout)
                if resource["kind"] == "Secret":
                    self.pod.create(body=resource,
                                    wait=wait,
                                    timeout=timeout)
                if resource["kind"] == "Service":
                    self.service.create(body=resource,
                                        wait=wait,
                                        timeout=timeout)
                else:
                    raise K8sInvalidResourceBody("unsupported resource type")


if __name__ == "__main__":
    pass
