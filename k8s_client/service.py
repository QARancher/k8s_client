import logging
from kubernetes.client import V1Service

from k8s_client.consts import DEFAULT_NAMESPACE
from k8s_client.utils import convert_obj_to_dict, field_filter, k8s_exceptions
from k8s_client.exceptions import K8sInvalidResourceBody, K8sException, \
    K8sNotFoundException

logger = logging.getLogger(__name__)


class ServiceClient(object):
    def __init__(self, client_core):
        self.client_core = client_core


    def wait_to_service_creation(self, service_name, namespace):
        """
        Wait to service creation
        :param service_name: the name of the service to wait for
        :type service_name: str
        :param namespace: the namespace of the service
        (default value is 'default')
        :type namespace: str
        """
        try:
            service = self.get(name=service_name, namespace=namespace)
            if not (hasattr(service, 'spec') and hasattr(service.spec, 'type')):
                return False
            if service.spec.type == "LoadBalancer":
                if not (hasattr(service, 'status') and hasattr(service.status,
                                                               'load_balancer') and hasattr(
                    service.status.load_balancer,
                    'ingress') and service.status.load_balancer.ingress is not None):
                    return False
                else:
                    return True
            else:
                return True
        except K8sNotFoundException:
            return False

    @k8s_exceptions
    def create(self, body, namespace=DEFAULT_NAMESPACE, wait=True):
        """
        Create service
        :param body: service's body
        :type body: dictionary or V1Service
        :param namespace: the namespace to create the service in if there is no
        namespace in the yaml (default value is 'default')
        :type namespace: str
        :param wait: to wait until the creation is over (default value is True)
        :type wait: bool
        :return: service name
        :rtype: str
        """
        try:
            if isinstance(body, V1Service):
                service_name = body.metadata.name
                if hasattr(body, "metadata") and hasattr(body.metadata,
                                                         "namespace"):
                    namespace = body.metadata.namespace
            elif isinstance(body, dict):
                service_name = body["metadata"]["name"]
                namespace = body.get("metadata", {}).get("namespace", namespace)
            else:
                raise K8sInvalidResourceBody()
        except (KeyError, AttributeError):
            raise K8sInvalidResourceBody()

        # create the service from the body
        self.client_core.create_namespaced_service(namespace=namespace,
                                                   body=body)
        logger.info(f"Created the service {service_name} in namespace "
                    "{namespace}")
        # wait to service creation
        if wait:
            self.wait_to_service_creation(service_name=service_name,
                                          namespace=namespace)
        return service_name


    def wait_to_service_deletion(self, service_name, namespace):
        """
        Wait until the service is deleted
        :param service_name: the name of the service
        :type service_name: str
        :param namespace: the namespace of the service
        (default value is 'default')
        :type namespace: str
        """
        try:
            self.get(name=service_name, namespace=namespace)
            return False
        except K8sNotFoundException:
            return True

    @k8s_exceptions
    def delete(self, name, namespace=DEFAULT_NAMESPACE, wait=True):
        """
        Delete service
        :param name: service's name
        :type name: str
        :param namespace: the namespace to delete the service from
        (default value is 'default')
        :type namespace: str
        :param wait: to wait until the deletion is over
        (default value is False)
        :type wait: bool
        """
        # delete the service
        self.client_core.delete_namespaced_service(name=name,
                                                   namespace=namespace)
        logger.info(f"Deleted {name} service from namespace {namespace}")
        # wait to the service to be deleted
        if wait:
            self.wait_to_service_deletion(service_name=name,
                                          namespace=namespace)

    @k8s_exceptions
    def get(self, name, namespace=DEFAULT_NAMESPACE, dict_output=False):
        """
        Return service obj or dictionary
        :param name: service name
        :type name: str
        :param namespace: the namespace of the service
        (default value is 'default')
        :type namespace: str
        :param dict_output: to return dictionary instead of obj
        :type dict_output: bool
        :return: the pod obj/dictionary
        :rtype: Union[V1Service,dictionary]
        """
        service = self.client_core.read_namespaced_service(name=name,
                                                           namespace=namespace)
        logger.info(f"Got {name} service from namespace {namespace}")

        # convert the obj to dict if required
        if dict_output:
            service = convert_obj_to_dict(service)
        else:
            service.metadata.resource_version = ''

        return service

    def get_ports(self, name, namespace):
        return self.get(name=name, namespace=namespace).spec.ports

    def get_cluster_ip(self, name, namespace):
        return self.get(name=name, namespace=namespace).spec.cluster_ip

    def get_external_ip(self, name, namespace):
        service = self.get(name=name, namespace=namespace)
        if hasattr(service, 'spec') and hasattr(service.spec,
                                                'type') and service.spec.type == "LoadBalancer":
            try:
                external_ip = service.status.load_balancer.ingress[0].ip
                return external_ip
            except (IndexError, AttributeError):
                raise K8sException(message="Could not find the external ip")
        else:
            raise K8sException(message="Only service from type load balancer"
                                       " has external ip")

    @k8s_exceptions
    def list(self, namespace=DEFAULT_NAMESPACE, all_namespaces=False,
             dict_output=False, field_selector=""):
        """
        Return list of services objects/dictionaries
        :param namespace: the namespace of the service
        (default value is 'default')
        :type namespace: str
        :param all_namespaces: to get the list from all the namespaces
        :type all_namespaces: bool
        :param dict_output: to get the elements of the list dictionaries
        instead of objects
        :type dict_output: bool
        :param field_selector: to filter the list to specific services
        :type field_selector: str
        :return: list of services
        :rtype: list
        """
        if all_namespaces:
            services_list = self.client_core.list_service_for_all_namespaces().items
            logger.info("Got services list from all the namespaces")
        else:
            services_list = self.client_core.list_namespaced_service(
                namespace=namespace).items
            logger.info(f"Got services list from namespace "
                        "{namespace}")
        if field_selector:
            services_list = field_filter(obj_list=services_list,
                                         field_selector=field_selector)
        # convert the list to list of dicts if required
        if dict_output:
            services_list = [convert_obj_to_dict(service) for service in
                             services_list]
        else:
            for service in services_list:
                service.metadata.resource_version = ''
        return services_list

    def list_names(self, namespace=DEFAULT_NAMESPACE, all_namespaces=False,
                   field_selector=""):
        return [service.metadata.name for service in
                self.list(namespace=namespace, all_namespaces=all_namespaces,
                          field_selector=field_selector)]

    @k8s_exceptions
    def events(self, name, namespace=DEFAULT_NAMESPACE, only_messages=False):
        """
        Return the list of the events of a specific service
        :param name: the name of the service
        :type name: str
        :param namespace: the namespace of the service
        (default value is 'default')
        :type namespace: str
        :param only_messages: to get only the events messages instead of
        getting all the objects
        :return: the list of the events
        :rtype: list
        """
        service_id = self.get(name=name, namespace=namespace).metadata.uid
        events = self.client_core.list_namespaced_event(namespace=namespace,
            field_selector=f"involvedObject.uid=={service_id}").items
        logger.info(f"Got the events of service {name} from namespace "
                    f"{namespace}")
        if only_messages:
            events = [event["message"] for event in events if
                      event.get("message") is not None]
        return events

    @k8s_exceptions
    def patch(self, name, body, namespace=DEFAULT_NAMESPACE):
        """
        Patch service
        :param name: the name of the service
        :type name: str
        :param body: the diff body to patch
        :type body: dictionary
        :param namespace: the namespace of the service
        (default value is 'default')
        :type namespace: str
        """
        logger.info(f"Patched service {name} from namespace {namespace}")
        self.client_core.patch_namespaced_service(name=name,
                                                  namespace=namespace,
                                                  body=body)


if __name__ == "__main__":
    pass
