import logging
from kubernetes.client import V1Pod
from kubernetes.stream import stream

from utils import convert_obj_to_dict, field_filter, k8s_exceptions, wait_for
from exceptions import (K8sInvalidResourceBody, K8sAuthenticationException,
                        K8sPullingException, K8sNotFoundException,
                        K8sRuntimeException)
from consts import (DEFAULT_NAMESPACE, COMPLETE_STATE, AUTHENTICATION_EXCEPTION,
                    PULLING_EXCEPTION, CREATED_SUCCESSFULLY, ERROR_STATE,
                    PULLING_FAIL)

logger = logging.getLogger(__name__)


class PodClient(object):
    def __init__(self, client_core):
        self.client_core = client_core

    @staticmethod
    def check_container_state(container_status, running_containers):
        """
        Check if the container is running by its status
        :param container_status: the status of the checked container
        :type container_status: dictionary
        :param running_containers: the current number of the running containers
        :type running_containers: int
        :return: running containers
        :rtype: int
        """
        message = None
        reason = None
        if container_status.get("running") is not None:
            running_containers += 1

        elif container_status.get("waiting") is not None:
            if container_status["waiting"].get("message") is not None:
                message = container_status["waiting"]["message"]
            if container_status["waiting"].get("reason") is not None:
                reason = container_status["waiting"]["reason"]

        elif container_status.get("terminated") is not None:
            if container_status["terminated"].get("message") is not None:
                message = container_status["terminated"]["message"]
            if container_status["terminated"].get("reason") is not None:
                reason = container_status["terminated"]["reason"]

        if message is not None and ERROR_STATE in message or reason is not None and ERROR_STATE in reason:
            raise K8sRuntimeException()
        if message is not None and COMPLETE_STATE in message or reason is not None and COMPLETE_STATE in reason:
            running_containers += 1

        return running_containers

    @k8s_exceptions
    def is_containers_started(self, pod_id, containers_counter,
                              namespace=DEFAULT_NAMESPACE):
        """
        Check if the all containers started
        :param pod_id: the id of the pod
        :type pod_id: str
        :param containers_counter: the number of the containers that have to
        start
        :type containers_counter: int
        :param namespace: the namespace of the pod (default value is 'default')
        :type namespace: str
        :return: True/False
        :rtype: bool
        """
        events_list = self.client_core.list_namespaced_event(
            namespace=namespace,
            field_selector="involvedObject.uid=={pod_uid}".format(
                pod_uid=pod_id))
        for event in events_list.items:
            if AUTHENTICATION_EXCEPTION in event.message:
                raise K8sAuthenticationException(message=event.message)
            if PULLING_EXCEPTION in event.message or PULLING_FAIL in event.message:
                raise K8sPullingException(message=event.message)
            if CREATED_SUCCESSFULLY in event.message:
                containers_counter -= 1
        return not containers_counter

    @wait_for
    def wait_for_containers_to_run(self, pod_name, pod_id, containers_counter,
                                   namespace=DEFAULT_NAMESPACE):
        """
        Wait until the containers are running
        :param pod_name: the name of the pod
        :type pod_name: str
        :param pod_id: the id of the pod
        :type pod_id: str
        :param containers_counter: the number of the containers to wait for
        (the number of the pod's containers)
        :type containers_counter: int
        :param namespace: the namespace of the pod (default value is 'default')
        :type namespace: str
        """
        if not self.is_containers_started(pod_id=pod_id,
                containers_counter=containers_counter, namespace=namespace):
            return False

        pod_dict = self.get(name=pod_name, namespace=namespace,
                            dict_output=True)
        if not pod_dict.get("status", {}).get("containerStatuses", []):
            return False
        container_statuses = pod_dict["status"]["containerStatuses"]
        if not all("state" in container_status for container_status in
                   container_statuses):
            return False
        else:
            running_containers = 0
            container_statuses = pod_dict["status"]["containerStatuses"]
            for container_status in container_statuses:
                running_containers_before = running_containers
                running_containers = PodClient.check_container_state(
                    container_status=container_status["state"],
                    running_containers=running_containers)
                if running_containers_before == running_containers and "lastState" in container_status:
                    running_containers = PodClient.check_container_state(
                        container_status=container_status["lastState"],
                        running_containers=running_containers)
                if running_containers_before == running_containers:
                    return False

            if running_containers == len(container_statuses):
                return True

    @k8s_exceptions
    def create(self, body, namespace=DEFAULT_NAMESPACE, wait=True):
        """
        Create pod
        :param body: pod's body
        :type body: dictionary or V1Pod
        :param namespace: the namespace to create the pod in if there is no
        namespace in the yaml (default value is 'default')
        :type namespace: str
        :param wait: to wait until the creation is over (default value is True)
        :type wait: bool
        :return: pod name
        :rtype: str
        """
        # check the type of the body and that it contains name
        # and raise exception if not
        try:
            if isinstance(body, V1Pod):
                pod_name = body.metadata.name
                if hasattr(body, "metadata") and hasattr(body.metadata,
                                                         "namespace"):
                    namespace = body.metadata.namespace
                containers_counter = len(body.spec.containers)
            elif isinstance(body, dict):
                pod_name = body["metadata"]["name"]
                namespace = body.get("metadata", {}).get("namespace", namespace)
                containers_counter = len(body["spec"]["containers"])
            else:
                raise K8sInvalidResourceBody()
        except (KeyError, AttributeError):
            raise K8sInvalidResourceBody()

        # create the pod from the body
        pod_obj = self.client_core.create_namespaced_pod(body=body,
                                                         namespace=namespace)
        logger.info(f"Created the pod {pod_name} in {namespace} namespace")
        # wait to the containers to run
        if wait:
            self.wait_for_containers_to_run(pod_name=pod_name,
                pod_id=pod_obj.metadata.uid,
                containers_counter=containers_counter, namespace=namespace)
        return pod_name

    @wait_for
    def wait_for_pod_to_be_deleted(self, pod_name, namespace=DEFAULT_NAMESPACE):
        """
        Wait until the pod is deleted
        :param pod_name: the name of the pod
        :type pod_name: str
        :param namespace: the namespace of the service
        (default value is 'default')
        :type namespace: str
        """
        try:
            self.get(name=pod_name, namespace=namespace)
            return False
        except K8sNotFoundException:
            return True

    @k8s_exceptions
    def delete(self, name, namespace=DEFAULT_NAMESPACE, wait=False):
        """
        Delete pod
        :param name: pod's name
        :type name: str
        :param namespace: the namespace to delete the pod from
        (default value is 'default')
        :type namespace: str
        :param wait: to wait until the deletion is over
        (default value is False)
        :type wait: bool
        """
        # delete the pod from the required namespace
        self.client_core.delete_namespaced_pod(name=name, namespace=namespace)
        logger.info(f"Deleted pod {name} from {namespace} namespace")
        # wait to the pod to be deleted
        if wait:
            self.wait_for_pod_to_be_deleted(pod_name=name, namespace=namespace)

    @k8s_exceptions
    def execute(self, name, command, command_prefix=None,
                namespace=DEFAULT_NAMESPACE, stderr=True, stdin=False,
                tty=False, container=None):
        """
        Execute command on pod
        :param name: the name of the pod
        :type name: str
        :param command: the command to run on the pod
        :type command: str
        :param command_prefix: the prefix to the command
        (default is ['sh', '-c'])
        :type command_prefix: list
        :param namespace: the namespace of the pod (default value is 'default')
        :type namespace: str
        :param stderr: get the error from the response (default value is True)
        :type stderr: bool
        :param stdin: get the input of the response (default value is False)
        :type stdin: bool
        :param tty: tty to get the response (default value is False)
        :type tty: bool
        :param container: specific container to run the command on
        (if there is only one it is not relevant)
        :type container: str
        :return: the response
        :rtype: str
        """
        command_prefix = command_prefix or ['sh', '-c']
        command = command_prefix + [command]
        kwargs = {"container": container} if container is not None else {}
        resp = stream(func=self.client_core.connect_post_namespaced_pod_exec,
                      name=name, namespace=namespace, command=command,
                      stdout=True, stderr=stderr, stdin=stdin, tty=tty,
                      **kwargs)
        logger.info("Executed {command} on pod {name} from namespace "
                    "{namespace}"
                    "{container}".format(command=command, name=name,
            namespace=namespace, container=" on container "
                                           "{container_name}"
                                           "".format(
                container_name=container) if container is not None else ""))
        return resp

    @k8s_exceptions
    def get(self, name, namespace=DEFAULT_NAMESPACE, dict_output=False):
        """
        Return pod obj or dictionary
        :param name: pod name
        :type name: str
        :param namespace: the namespace of the pod (default value is 'default')
        :type namespace: str
        :param dict_output: to return dictionary instead of obj
        :type dict_output: bool
        :return: the pod obj/dictionary
        :rtype: Union[V1Pod,dictionary]
        """
        pod = self.client_core.read_namespaced_pod(name=name,
                                                   namespace=namespace)
        logger.info(f"Got pod {name} from {namespace} namespace")

        # convert the obj to dict if required
        if dict_output:
            pod = convert_obj_to_dict(pod)
        else:
            pod.metadata.resource_version = ''

        return pod

    def get_ip(self, name, namespace=DEFAULT_NAMESPACE):
        return self.get(name=name, namespace=namespace).status.pod_ip

    def get_host_internal_ip(self, name, namespace=DEFAULT_NAMESPACE):
        return self.get(name=name, namespace=namespace).status.host_ip

    def get_status(self, name, namespace=DEFAULT_NAMESPACE):
        return self.get(name=name, namespace=namespace).status.phase

    def get_uid(self, name, namespace=DEFAULT_NAMESPACE):
        return self.get(name=name, namespace=namespace).metadata.uid

    def get_name(self, uid, namespace=None):
        return self.list(namespace=namespace, all_namespaces=namespace is None,
                         field_selector=f"metadata.uid=={uid}")

    @k8s_exceptions
    def list(self, namespace=DEFAULT_NAMESPACE, all_namespaces=False,
             dict_output=False, field_selector=""):
        """
        Return list of pods objects/dictionaries
        :param namespace: the namespace of the pod (default value is 'default')
        :type namespace: str
        :param all_namespaces: to get the list from all the namespaces
        :type all_namespaces: bool
        :param dict_output: to get the elements of the list dictionaries
        instead of objects
        :type dict_output: bool
        :param field_selector: to filter the list to specific pods
        :type field_selector: str
        :return: list of pods
        :rtype: list
        """
        if all_namespaces:
            pods_list = self.client_core.list_pod_for_all_namespaces().items
            logger.info("Got the pods list from all the namespaces")
        else:
            pods_list = self.client_core.list_namespaced_pod(
                namespace=namespace).items
            logger.info(f"Got the pods list from {namespace} namespace")

        if field_selector:
            pods_list = field_filter(obj_list=pods_list,
                                     field_selector=field_selector)

        # convert the list to list of dicts if required
        if dict_output:
            pods_list = [convert_obj_to_dict(pod) for pod in pods_list]
        else:
            for pod in pods_list:
                pod.metadata.resource_version = ''

        return pods_list

    def list_names(self, namespace=DEFAULT_NAMESPACE, all_namespaces=False,
                   field_selector=""):
        return [pod.metadata.name for pod in
                self.list(namespace=namespace, all_namespaces=all_namespaces,
                          field_selector=field_selector)]

    @k8s_exceptions
    def logs(self, name, namespace=DEFAULT_NAMESPACE, container=None):
        """
        Return pod's logs
        :param name: the name of the pod
        :type name: str
        :param namespace: the namespace of the pod (default value is 'default')
        :type namespace: str
        :param container: specific container to get the logs from
        (if there is only one it is not relevant)
        :return: the pod's logs
        :rtype: str
        """
        logger.info("Got logs of pod {name} from namespace {namespace}"
                    "{container}".format(name=name, namespace=namespace,
            container=" of {container_name}"
                      "".format(
                container_name=container) if container is not None else ""))
        kwargs = {"container": container} if container is not None else {}
        return self.client_core.read_namespaced_pod_log(name=name,
                                                        namespace=namespace,
                                                        **kwargs)

    @k8s_exceptions
    def events(self, name, namespace=DEFAULT_NAMESPACE, only_messages=False):
        """
        Return the list of the events of a specific pod
        :param name: the name of the pod
        :type name: str
        :param namespace: the namespace of the pod (default value is 'default')
        :type namespace: str
        :param only_messages: to get only the events messages instead of
        getting all the objects
        :return: the list of the events
        :rtype: list
        """
        pod_id = self.get_uid(name=name, namespace=namespace)
        events = self.client_core.list_namespaced_event(namespace=namespace,
            field_selector=f"involvedObject.uid=={pod_id}").items
        logger.info(f"Got the events of pod {name} from namespace {namespace}")
        if only_messages:
            events = [event["message"] for event in events if
                      event.get("message") is not None]
        return events

    @k8s_exceptions
    def patch(self, name, body, namespace=DEFAULT_NAMESPACE):
        """
        Patch pod
        :param name: the name of the pod
        :type name: str
        :param body: the diff body to patch
        :type body: dictionary
        :param namespace: the namespace of the pod (default value is 'default')
        :type namespace: str
        """
        logger.info(f"Patched pod {name} from namespace {namespace}")
        self.client_core.patch_namespaced_pod(name=name, namespace=namespace,
                                              body=body)


if __name__ == "__main__":
    pass
