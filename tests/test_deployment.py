import pytest

from helpers.k8s_deployment import K8sDeployment
from helpers.k8s_image import K8sImage
from tests.asserts_wrapper import assert_not_none, assert_equal, assert_in_list
from tests.basetest import BaseTest

mysql_image_obj = K8sImage(image_name="mysql", version="8.0.0")
alpine_image_obj = K8sImage(image_name="alpine", version="latest")


@pytest.mark.parametrize("image", [alpine_image_obj, mysql_image_obj],
                         ids=[alpine_image_obj.image_name, mysql_image_obj.image_name])
class TestK8sDeployment(BaseTest):
    """
    Test class for functionality tests of deployment.
    multiple images can be run for each test, use @pytest.mark.parametrize to
    pass it a list of image object (K8sImage)
    Steps:
    0. Create namespace  with hte name test. uses fixture 'create_namespace'
    1. Create deployment and verify that the deployment is running by checking
    the status of the deployment, returned in the k8s object.
    2. Get list of all deployments running in name space, verify that the
    created images are in the list.
    3. Get list of names of deployments, verify that all created images are in
    the list name
    4. Get pods of deployments
    """

    @pytest.mark.dependency(name="create_deployment")
    def test_create_deployment(self, orc, create_namespace, image):
        deployment_obj = K8sDeployment(name=image.image_name)
        deployment_obj.namespace = create_namespace
        deployment_obj.labels = {"app": image.image_name}
        deployment_obj.selector.update(
            {"matchLabels": {"app": image.image_name}})
        deployment_obj.add_container_to_deployment(image_obj=image,
                                                   command="sleep 99")
        res = orc.deployment.create(body=deployment_obj, max_threads=5)
        assert_not_none(actual_result=res)
        res = orc.deployment.get(name=image.image_name,
                                 namespace=create_namespace)
        assert_equal(actual_result=res.metadata.name,
                     expected_result=image.image_name)

    @pytest.mark.dependency(name="deployment_list",
                            depends=["create_deployment"])
    def test_list_deployments(self, orc, image, create_namespace):
        dep_list = orc.deployment.list(all_namespaces=True)
        assert_not_none(actual_result=dep_list)
        filtered_dep_list = [dep.status.available_replicas for dep in dep_list
                             if image.image_name in dep.metadata.name]
        assert_not_none(actual_result=filtered_dep_list)
        assert_in_list(searched_list=filtered_dep_list, wanted_element=1)

    @pytest.mark.dependency(name="deployments_names_list",
                            depends=["create_deployment"])
    def test_list_names_deployments(self, orc, image, create_namespace):
        dep_list = orc.deployment.list_names(namespace=create_namespace)
        assert_not_none(actual_result=dep_list)
        assert_in_list(searched_list=dep_list, wanted_element=image.image_name)

    @pytest.mark.dependency(name="get_deployment",
                            depends=["create_deployment"])
    def test_get_deployment(self, orc, image, create_namespace):
        dep = orc.deployment.get(name=image.image_name,
                                 namespace=create_namespace)
        assert_not_none(actual_result=dep)
        assert_equal(actual_result=dep.metadata.name,
                     expected_result=image.image_name)

    @pytest.mark.dependency(name="get_pods_of_deployment",
                            depends=["create_deployment"])
    def test_get_pods_of_deployment(self, orc, image, create_namespace):
        pod_list = orc.deployment.get_pods(name=image.image_name,
                                           namespace=create_namespace)
        assert_not_none(actual_result=pod_list)
        for pod in pod_list:
            assert_equal(actual_result=pod.status.phase,
                         expected_result="Running",
                         message=f"Pod {pod.metadata.name} is not running "
                                 f"for deployment: {image.image_name} "
                                 f"in namespace: {create_namespace}")

