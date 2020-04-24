import pytest

from helpers.k8s_namespace import K8sNamespace
from lite_k8s import K8sClient
from tests.consts import NAMESPACE_BYPASS_NAMES


@pytest.fixture(scope="class")
def orc():
    return K8sClient()


@pytest.fixture(scope="function")
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


def delete_deployment(orc, deployment_name, deployment_namespace):
    orc.deployment.delete(name=deployment_name, namespace=deployment_namespace,
                          wait=True)


def is_namespace_allowed(name):
    """
    function to verify if the name of k8s namespace can be deleted,
    or it a part of the k8s infrastructures.
    verified against list of names in test/consts.py
    """
    if name not in NAMESPACE_BYPASS_NAMES:
        return True
    return False


def delete_all_namespaces(orc):
    namespaces_names = orc.namespace.list_names()
    ns_name = iter(namespaces_names)
    if is_namespace_allowed(ns_name):
        delete_namespace(orc=orc, namespace_name=next(ns_name))


def delete_all_deployments(orc):
    namespaces_names = orc.namespace.list_names()
    ns_name = iter(namespaces_names)
    if is_namespace_allowed(ns_name):
        deployments_names = orc.deployment.list_names()
        deployment_name = iter(deployments_names)
        delete_deployment(orc=orc, deployment_name=deployment_name,
                          deployment_namespace=ns_name)


@pytest.fixture(scope="class")
def clean_all(orc, request):
    def teardown():
        delete_all_deployments(orc=orc)
        delete_all_namespaces(orc=orc)

    request.addfinalizer(teardown)

