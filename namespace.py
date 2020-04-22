import logging
from kubernetes.client import V1Namespace


from consts import WAIT_TIMEOUT
from utils import convert_obj_to_dict, field_filter, k8s_exceptions, wait_for
from exceptions import K8sInvalidResourceBody, K8sNotFoundException
logger = logging.getLogger(__name__)


class NamespaceClient(object):
    def __init__(self,
                 client_core):
        self.client_core = client_core

    @wait_for
    def wait_to_namespace_creation(self,
                                   namespace_name):
        """
        Wait to namespace creation
        :param namespace_name: the name of the namespace to wait for
        :type namespace_name: str
        """
        try:
            self.get(name=namespace_name)
            return True
        except K8sNotFoundException:
            return False

    @k8s_exceptions
    def create(self,
               body,
               wait=True):
        """
        Create namespace
        :param body: namespace's body
        :type body: dictionary or V1Namespace
        :param wait: to wait until the creation is over (default value is True)
        :type wait: bool
        :return: namespace name
        :rtype: str
        """
        try:
            if isinstance(body, V1Namespace):
                namespace_name = body.metadata.name
            elif isinstance(body, dict):
                namespace_name = body["metadata"]["name"]
            else:
                raise K8sInvalidResourceBody()
        except (KeyError, AttributeError):
            raise K8sInvalidResourceBody()
        # create the namespace from the body
        self.client_core.create_namespace(body=body)
        logger.info(f"Created the namespace {namespace_name}")
        # wait to namespace creation
        if wait:
            self.wait_to_namespace_creation(namespace_name=namespace_name)
        return namespace_name

    @wait_for
    def wait_to_namespace_deletion(self,
                                   namespace_name):
        """
        Wait until the namespace is deleted
        :param namespace_name: the name of the namespace
        :type namespace_name: str
        """
        try:
            self.get(name=namespace_name)
            return False
        except K8sNotFoundException:
            return True

    @k8s_exceptions
    def delete(self,
               name,
               wait=False):
        """
        Delete namespace
        :param name: namespace's name
        :type name: str
        :param wait: to wait until the deletion is over
        (default value is False)
        :type wait: bool
        """
        # delete the namespace
        self.client_core.delete_namespace(name=name)
        logger.info(f"Deleted {name} namespace")

        # wait to the namespace to be deleted
        if wait:
            self.wait_to_namespace_deletion(namespace_name=name)

    @k8s_exceptions
    def get(self,
            name,
            dict_output=False):
        """
        Return namespace obj or dictionary
        :param name: namespace name
        :type name: str
        :param dict_output: to return dictionary instead of obj
        :type dict_output: bool
        :return: the namespace obj/dictionary
        :rtype: Union[V1Namespace,dictionary]
        """
        namespace = self.client_core.read_namespace(name=name)
        logger.info(f"Got namespace {name}")

        # convert the obj to dict if required
        if dict_output:
            namespace = convert_obj_to_dict(namespace)
        else:
            namespace.metadata.resource_version = ''

        return namespace

    @k8s_exceptions
    def list(self,
             dict_output=False,
             field_selector=""):
        """
        Return list of namespaces objects/dictionaries
        :param dict_output: to get the elements of the list dictionaries
        instead of objects
        :type dict_output: bool
        :param field_selector: to filter the list to specific namespaces
        :type field_selector: str
        :return: list of namespaces
        :rtype: list
        """
        namespaces_list = self.client_core.list_namespace().items
        logger.info("Got namespaces")

        if field_selector:
            namespaces_list = field_filter(obj_list=namespaces_list,
                                           field_selector=field_selector)
        # convert the list to list of dicts if required
        if dict_output:
            namespaces_list = [convert_obj_to_dict(namespace)
                               for namespace in namespaces_list]
        else:
            for namespace in namespaces_list:
                namespace.metadata.resource_version = ''
        return namespaces_list

    def list_names(self,
                   field_selector=""):
        return [namespace.metadata.name
                for namespace in self.list(field_selector=field_selector)]


if __name__ == "__main__":
    pass
