import logging
from kubernetes.client import V1Secret

from k8s_client.consts import DEFAULT_NAMESPACE
from k8s_client.utils import convert_obj_to_dict, field_filter, k8s_exceptions
from k8s_client.exceptions import K8sInvalidResourceBody, K8sNotFoundException

logger = logging.getLogger(__name__)


class SecretClient(object):
    def __init__(self, client_core):
        self.client_core = client_core


    def wait_to_secret_creation(self, secret_name, namespace):
        """
        Wait to secret creation
        :param secret_name: the name of the secret to wait for
        :type secret_name: str
        :param namespace: the namespace of the secret
        (default value is 'default')
        :type namespace: str
        """
        try:
            self.get(name=secret_name, namespace=namespace)
            return True
        except K8sNotFoundException:
            return False

    @k8s_exceptions
    def create(self, body, namespace=DEFAULT_NAMESPACE, wait=True):
        """
        Create a secret
        :param body: secret's body
        :type body: dictionary or V1Secret
        :param namespace: the namespace to create the secret in if there is no
        namespace in the yaml (default value is 'default')
        :type namespace: str
        :param wait: to wait until the creation is over (default value is True)
        :type wait: bool
        :return: secret name
        :rtype: str
        """
        try:
            if isinstance(body, V1Secret):
                secret_name = body.metadata.name
                if hasattr(body, "metadata") and hasattr(body.metadata,
                                                         "namespace"):
                    namespace = body.metadata.namespace
            elif isinstance(body, dict):
                secret_name = body["metadata"]["name"]
                namespace = body.get("metadata", {}).get("namespace", namespace)
            else:
                raise K8sInvalidResourceBody()
        except (KeyError, AttributeError):
            raise K8sInvalidResourceBody()
        # create the secret from the body
        self.client_core.create_namespaced_secret(namespace=namespace,
                                                  body=body)
        logger.info(
            f"Created the secret {secret_name} in namespace {namespace}")

        # wait to secret creation
        if wait:
            self.wait_to_secret_creation(secret_name=secret_name,
                                         namespace=namespace)
        return secret_name


    def wait_to_secret_deletion(self, secret_name, namespace):
        """
        Wait until the secret is deleted
        :param secret_name: the name of the secret
        :type secret_name: str
        :param namespace: the namespace of the secret
        (default value is 'default')
        :type namespace: str
        """
        try:
            self.get(name=secret_name, namespace=namespace)
            return False
        except K8sNotFoundException:
            logger.info("Finished waiting before the timeout")
            return True

    @k8s_exceptions
    def delete(self, name, namespace=DEFAULT_NAMESPACE, wait=False):
        """
        Delete secret
        :param name: secret's name
        :type name: str
        :param namespace: the namespace to delete the secret from
        (default value is 'default')
        :type namespace: str
        :param wait: to wait until the deletion is over (default value is False)
        :type wait: bool
        """
        # delete the secret
        self.client_core.delete_namespaced_secret(name=name,
                                                  namespace=namespace)
        logger.info(f"Deleted {name} secret from namespace {namespace}")
        # wait to the secret to be deleted
        if wait:
            self.wait_to_secret_deletion(secret_name=name, namespace=namespace)

    @k8s_exceptions
    def get(self, name, namespace=DEFAULT_NAMESPACE, dict_output=False):
        """
        Return secret obj or dictionary
        :param name: secret name
        :type name: str
        :param namespace: the namespace of the secret
        (default value is 'default')
        :type namespace: str
        :param dict_output: to return dictionary instead of obj
        :type dict_output: bool
        :return: the pod obj/dictionary
        :rtype: Union[V1Secret,dictionary]
        """
        secret = self.client_core.read_namespaced_secret(name=name,
                                                         namespace=namespace)
        logger.info(f"Got {name} secret from namespace {namespace}")
        # convert the obj to dict if required
        if dict_output:
            secret = convert_obj_to_dict(secret)
        else:
            secret.metadata.resource_version = ''
        return secret

    @k8s_exceptions
    def list(self, namespace=DEFAULT_NAMESPACE, all_namespaces=False,
             dict_output=False, field_selector=""):
        """
        Return list of secrets objects/dictionaries
        :param namespace: the namespace of the secret
        (default value is 'default')
        :type namespace: str
        :param all_namespaces: to get the list from all the namespaces
        :type all_namespaces: bool
        :param dict_output: to get the elements of the list dictionaries
        instead of objects
        :type dict_output: bool
        :param field_selector: to filter the list to specific secrets
        :type field_selector: str
        :return: list of secrets
        :rtype: list
        """
        if all_namespaces:
            secrets_list = self.client_core.list_secret_for_all_namespaces().items
            logger.info("Got secrets list from all the namespaces")
        else:
            secrets_list = self.client_core.list_namespaced_secret(
                namespace=namespace).items
            logger.info(f"Got secrets list from namespace "
                        f"{namespace}")

        if field_selector:
            secrets_list = field_filter(obj_list=secrets_list,
                                        field_selector=field_selector)

        # convert the list to list of dicts if required
        if dict_output:
            secrets_list = [convert_obj_to_dict(secret) for secret in
                            secrets_list]
        else:
            for secret in secrets_list:
                secret.metadata.resource_version = ''

        return secrets_list

    def list_names(self, namespace=DEFAULT_NAMESPACE, all_namespaces=False,
                   field_selector=""):
        return [secret.metadata.name for secret in
                self.list(namespace=namespace, all_namespaces=all_namespaces,
                          field_selector=field_selector)]

    @k8s_exceptions
    def patch(self, name, body, namespace=DEFAULT_NAMESPACE):
        """
        Patch secret
        :param name: the name of the secret
        :type name: str
        :param body: the diff body to patch
        :type body: dictionary
        :param namespace: the namespace of the secret
        (default value is 'default')
        :type namespace: str
        """
        logger.info(f"Patched {name} secret from namespace {namespace}")
        self.client_core.patch_namespaced_secret(name=name, namespace=namespace,
                                                 body=body)


if __name__ == "__main__":
    pass
