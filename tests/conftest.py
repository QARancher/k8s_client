import pytest

from helpers.k8s_namespace import K8sNamespace
from lite_k8s import K8sClient


@pytest.fixture(scope="class")
def orc():
    return K8sClient()


def create_namespace(orc, namespace_name):
    if not orc.namespace.get(name=namespace_name):
        ns_obj = K8sNamespace(name=namespace_name)
        orc.namespace.create(body=ns_obj)