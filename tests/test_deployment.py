import pytest

from consts import DEFAULT_NAMESPACE
from helpers.k8s_deployment import K8sDeployment
from helpers.k8s_image import K8sImage
from tests.asserts_wrapper import assert_not_none, assert_equal
from tests.basetest import BaseTest

image_obj = K8sImage(image_name="mysql", version="8.0.0")


class TestK8sDeployment(BaseTest):
    @pytest.mark.parametrize("image", [image_obj], ids=[image_obj.image_name])
    def test_create_deployment(self, orc, create_namespace, image):
        deployment_obj = K8sDeployment(name=image.image_name)
        deployment_obj.namespace = create_namespace
        deployment_obj.labels = {"app": image.image_name}
        deployment_obj.selector.update(
            {"matchLabels": {"app": image.image_name}})
        deployment_obj.add_container_to_deployment(image_obj=image,
            ports_list=[(3306, "TCP")])
        res = orc.deployment.create(body=deployment_obj)
        assert_not_none(actual_result=res)
        res = orc.deployment.get(name=image.image_name)
        assert_equal(actual_result=res, expected_result=image.image_name)

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
