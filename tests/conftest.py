import pytest

from helpers.k8s_namespace import K8sNamespace
from lite_k8s import K8sClient


@pytest.fixture(scope="class")
def orc():
    return K8sClient()


@pytest.fixture(scope="class")
def create_namespace(orc, request):
    ns_name = "test"
    ns_obj = K8sNamespace(name=ns_name)
    # create namespace
    ns = orc.namespace.create(body=ns_obj)

    def teardown():
        delete_namespace(orc=orc, namespace_name=ns_name)

    request.addfinalizer(teardown)
    return ns



def delete_namespace(orc, namespace_name):
    orc.namespace.delete(name=namespace_name, wait=True)
