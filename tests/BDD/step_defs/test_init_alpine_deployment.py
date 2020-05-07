from pytest_bdd import scenarios, given, when, then, parsers

from helpers.k8s_deployment import K8sDeployment
from helpers.k8s_image import K8sImage
from helpers.k8s_namespace import K8sNamespace


@given(parsers.parse("Given namespace {ns_name} created"))
def create_namespace(k8s_client, ns_name):
    ns_obj = K8sNamespace(name=ns_name)
    ns = k8s_client.namespace.create(body=ns_obj)
    return ns


@when(parsers.parse("deployment {deployment_name:version} is started"))
def start_deployment(k8s_client, deployment_name, version):
    image_obj = K8sImage(image_name=deployment_name, version=version)
    deployment_obj = K8sDeployment(name=image_obj.image_name)
    deployment_obj.namespace = create_namespace
    deployment_obj.labels = {"app": image_obj.image_name}
    deployment_obj.selector.update(
        {"matchLabels": {"app": image_obj.image_name}})
    deployment_obj.add_container_to_deployment(image_obj=image_obj,
                                               command="sleep 99")
    k8s_client.deployment.create(body=deployment_obj)
