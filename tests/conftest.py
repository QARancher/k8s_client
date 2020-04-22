import pytest

from helpers.k8s_namespace import K8sNamespace
from lite_k8s import K8sClient


@pytest.fixture(scope="class")
def orc():
    return K8sClient()