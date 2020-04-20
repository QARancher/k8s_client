from helpers.k8s_namespace import K8sNamespace
from tests.asserts_wrapper import assert_not_none, assert_in_list, \
    assert_not_in_list, assert_equal
from tests.basetest import BaseTest


class TestNamespace(BaseTest):
    def test_get_namespaces_names(self, orc):
        # list of names
        ns_names = orc.namespace.list_names()
        assert_not_none(actual_result=ns_names)
        assert_in_list(wanted_element="default", list=ns_names)

    def test_get_namespaces_list(self, orc):
        ns_list = orc.namespace.list()
        assert_not_none(ns_list)

    def test_get_namespace(self, orc):
        ns = orc.namespace.get(name="default")
        assert_not_none(actual_result=ns,
                        message="Failed to get default namespace")

    def test_create_and_delete_namespace(self, orc):
        ns_name = "test"
        # verify that namespace doesn't exists
        ns_names = orc.namespace.list_names()
        assert_not_in_list(list=ns_names,
                           unwanted_element=ns_name)
        ns_obj = K8sNamespace(name=ns_name)
        # create namespace
        orc.namespace.create(ns_obj)
        ns = orc.namespace.get(name=ns_name)
        assert_equal(actual_result=ns.metadata.name, expected_result=ns_name)
        # delete namespace
        orc.namespace.delete(name=ns, wait=True)
        assert_not_in_list(list=orc.namespace.list_names(),
                           unwanted_element=ns_name)
