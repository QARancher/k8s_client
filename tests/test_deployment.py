import pytest

from tests.asserts_wrapper import assert_not_none
from tests.basetest import BaseTest


class TestK8sDeployment(BaseTest):
    def test_list_deployments(self, orc):
        dep_list = orc.deployment.list(all_namespaces=True)
        assert_not_none(actual_result=dep_list)

    def test_list_names_deployments(self, orc):
        dep_list = orc.deployment.list_names()
        assert_not_none(actual_result=dep_list)

    def test_get_deployment(self, orc):
        pass

    def test_get_pods_of_deployment(self, orc):
        pass

    def test_create_deployment(self, orc, create_namespace):
        pass

