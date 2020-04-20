import pytest

from lite_k8s import K8sClient


@pytest.fixture(scope="class")
def orc():
    return K8sClient()
