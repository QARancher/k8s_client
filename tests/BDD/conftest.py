import pytest

from k8s_client.lite_k8s import K8sClient


@pytest.fixture(scope="class")
def k8s_client():
    return K8sClient()
