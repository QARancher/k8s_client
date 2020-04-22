from consts import DEFAULT_NAMESPACE
from exceptions import K8sAlreadyExistsException, K8sNotFoundException
from helpers.k8s_namespace import K8sNamespace
from tests.asserts_wrapper import assert_not_none, assert_in_list, \
    assert_not_in_list, assert_equal
from tests.basetest import BaseTest


class TestNamespace(BaseTest):
    def test_get_namespaces_names(self, orc):
        # list of names
        ns_names = orc.namespace.list_names()
        assert_not_none(actual_result=ns_names)
        assert_in_list(wanted_element="default", searched_list=ns_names)

    def test_get_namespaces_list(self, orc):
        ns_list = orc.namespace.list()
        assert_not_none(ns_list)

    def test_get_namespace(self, orc):
        ns = orc.namespace.get(name=DEFAULT_NAMESPACE)
        assert_not_none(actual_result=ns,
                        message=f"Failed to get {DEFAULT_NAMESPACE} namespace")

    def test_create_and_delete_namespace(self, orc):
        ns_name = "test"
        ns_obj = K8sNamespace(name=ns_name)
        # create namespace
        orc.namespace.create(body=ns_obj)
        ns = orc.namespace.get(name=ns_name)
        assert_equal(actual_result=ns.metadata.name, expected_result=ns_name)
        # delete namespace
        orc.namespace.delete(name=ns_name, wait=True)
        assert_not_in_list(searched_list=orc.namespace.list_names(),
                           unwanted_element=ns_name)

    def test_create_already_exists_namespace(self, orc):
        ns_name = "test"
        ns_obj = K8sNamespace(name=ns_name)
        # create namespace
        orc.namespace.create(body=ns_obj)
        try:
            orc.namespace.create(body=ns_obj)
            raise AssertionError("Did not get exception K8sAlreadyExists")
        except K8sAlreadyExistsException:
            pass

    def test_delete_incorrect_namespace(self, orc):
        ns_name = "test1"
        ns_obj = K8sNamespace(name=ns_name)
        # create namespace
        try:
            orc.namespace.delete(ns_obj)
            raise AssertionError("Did not get exception K8sNotFoundException")
        except K8sNotFoundException:
            pass

