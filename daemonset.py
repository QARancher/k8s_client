import logging
from kubernetes.client import V1DaemonSet


from commons.decorators import poll_timeout
from framework.utils.decorators import k8s_exceptions
from lite_k8s_cli.exceptions import K8sInvalidResourceBody
from lite_k8s_cli.utils import convert_obj_to_dict, field_filter
from lite_k8s_cli.consts import (
    DEFAULT_NAMESPACE,
    WAIT_TIMEOUT,
    DEFAULT_MAX_THREADS
)


logger = logging.getLogger(__name__)


class DaemonSetClient(object):

    def __init__(self,
                 client_app,
                 pod,
                 deployment):
        self.client_app = client_app
        self.pod = pod
        self.deployment = deployment

    def finished_to_create_ready_replicas(self,
                                          name,
                                          namespace):
        """
        Return if the pods of a daemon set are created
        :param name: the name of the daemon set
        :type name: str
        :param namespace: the namespace of the daemon set
        (default value is 'default')
        :type namespace: str
        :return: created or not (True/False)
        :rtype: bool
        """
        daemon_set = self.get(name=name,
                              namespace=namespace)
        return (daemon_set.status.desired_number_scheduled ==
                daemon_set.status.current_number_scheduled)

    @poll_timeout(default_timeout=WAIT_TIMEOUT,
                  log="Wait to {daemon_set_name}'s pod to create with "
                      "{timeout} timeout")
    def wait_for_daemon_set_to_run(self,
                                   daemon_set_name,
                                   namespace=DEFAULT_NAMESPACE,
                                   timeout=WAIT_TIMEOUT,
                                   max_threads=DEFAULT_MAX_THREADS):
        """
        Wait until the daemon set is running (including their pods...)
        :param daemon_set_name: the name of the daemon set
        :type daemon_set_name: str
        :param namespace: the namespace of the daemon set
        (default value is 'default')
        :type namespace: str
        :param timeout: timeout to wait to the creation
        (default value is WAIT_TIMEOUT)
        :type timeout: int
        :param max_threads: max number of threads to open during waiting
        (default value is DEFAULT_MAX_THREADS)
        :type max_threads: int
        """
        if not self.finished_to_create_ready_replicas(name=daemon_set_name,
                                                      namespace=namespace):
            return False
        self.deployment.wait_for_pods_creation_thread_manager(
            pods=self.get_pods(
                name=daemon_set_name,
                namespace=namespace),
            namespace=namespace,
            timeout=timeout,
            max_threads=max_threads)
        logger.info("Finished waiting before the timeout {timeout}"
                    "".format(timeout=timeout))
        return True

    @k8s_exceptions
    def create(self,
               body,
               namespace=DEFAULT_NAMESPACE,
               wait=True,
               timeout=WAIT_TIMEOUT,
               max_threads=DEFAULT_MAX_THREADS):
        """
        Create daemon set
        :param body: daemon set's body
        :type body: dictionary or V1DaemonSet
        :param namespace: the namespace to create the daemon set in if there is
        no namespace in the yaml (default value is 'default')
        :type namespace: str
        :param wait: to wait until the creation (default value is True)
        :type wait: bool
        :param timeout: timeout to wait to the creation is over
        (default value is WAIT_TIMEOUT)
        :type timeout: int
        :param max_threads: max number of threads to open during waiting
        (default value is DEFAULT_MAX_THREADS)
        :type max_threads: int
        :return: daemon set name
        :rtype: str
        """
        # check the type of the body and that it contains name
        # and raise exception if not
        try:
            if isinstance(body, V1DaemonSet):
                daemon_set_name = body.metadata.name
                if hasattr(body, "metadata") and \
                        hasattr(body.metadata, "namespace"):
                    namespace = body.metadata.namespace
            elif isinstance(body, dict):
                daemon_set_name = body["metadata"]["name"]
                namespace = body.get("metadata", {}).get("namespace", namespace)
            else:
                raise K8sInvalidResourceBody()
        except (KeyError, AttributeError):
            raise K8sInvalidResourceBody()

        # create the daemon from the body
        self.client_app.create_namespaced_daemon_set(
            body=body,
            namespace=namespace)
        logger.info("Created the daemon set {daemon_set_name} in {namespace} "
                    "namespace".format(daemon_set_name=daemon_set_name,
                                       namespace=namespace)
                    )

        # wait to the daemon set to run
        if wait:
            self.wait_for_daemon_set_to_run(daemon_set_name=daemon_set_name,
                                            namespace=namespace,
                                            timeout=timeout,
                                            max_threads=max_threads)
        return daemon_set_name

    @k8s_exceptions
    def delete(self,
               name,
               namespace=DEFAULT_NAMESPACE,
               wait=False,
               timeout=WAIT_TIMEOUT,
               max_threads=DEFAULT_MAX_THREADS):
        """
        Delete daemon set
        :param name: daemon set's name
        :type name: str
        :param namespace: the namespace to delete the daemon set from
        (default value is 'default')
        :type namespace: str
        :param wait: to wait until the deletion is over
        (default value is False)
        :type wait: bool
        :param timeout: timeout to wait to the deletion
        (default value is WAIT_TIMEOUT)
        :type timeout: int
        :param max_threads: max number of threads to open during waiting
        (default value is DEFAULT_MAX_THREADS)
        :type: max_threads: int
        """
        # get pods before the deleting
        pods = self.get_pods(name=name,
                             namespace=namespace)

        # delete the pod from the required namespace
        self.client_app.delete_namespaced_daemon_set(name=name,
                                                     namespace=namespace)
        logger.info("Deleted daemon set {name} from {namespace} namespace"
                    "".format(name=name,
                              namespace=namespace))

        # wait to the pods to be deleted
        if wait:
            logger.info("Wait to {daemon_set_name} to be deleted with "
                        "{timeout} timeout".format(daemon_set_name=name,
                                                   timeout=timeout)
                        )
            self.deployment.wait_for_pods_to_be_deleted_thread_manager(
                pods=pods,
                namespace=namespace,
                timeout=timeout,
                max_threads=max_threads)

    @poll_timeout(default_timeout=WAIT_TIMEOUT,
                  log="Wait to daemon set {name} from namespace {namespace} "
                      "to be patched with {timeout} timeout")
    def wait_for_daemon_set_to_patch(self,
                                     name,
                                     pods,
                                     namespace=DEFAULT_NAMESPACE,
                                     timeout=WAIT_TIMEOUT,
                                     max_threads=DEFAULT_MAX_THREADS):
        """
        Wait until the daemon set's pods are patched
        :param name: the name of the daemon set
        :type name: str
        :param pods: the daemon set's pods
        :type pods: list
        :param namespace: the namespace of the daemon set
        (default value is 'default')
        :type namespace: str
        :param timeout: timeout to wait to the patch
        (default value is WAIT_TIMEOUT)
        :type timeout: int
        :param max_threads: max number of threads to open during waiting
        (default value is DEFAULT_MAX_THREADS)
        :type max_threads: int
        """
        self.deployment.wait_for_pods_to_be_deleted_thread_manager(
            pods=pods,
            namespace=namespace,
            timeout=timeout,
            max_threads=max_threads)
        self.wait_for_daemon_set_to_run(deployment_name=name,
                                        namespace=namespace,
                                        timeout=timeout,
                                        max_threads=max_threads)
        logger.info("Finished waiting before the timeout {timeout}"
                    "".format(timeout=timeout))
        return True

    @k8s_exceptions
    def patch(self,
              name,
              body,
              namespace=DEFAULT_NAMESPACE,
              wait=True,
              timeout=WAIT_TIMEOUT,
              max_threads=DEFAULT_MAX_THREADS):
        """
        Patch daemon set
        :param name: the name of the daemon set
        :type name: str
        :param body: the diff body to patch
        :type body: dictionary
        :param namespace: the namespace of the daemon set
        (default value is 'default')
        :type namespace: str
        :param wait: to wait until the patch is over
        (default value is True)
        :type wait: bool
        :param timeout: timeout to wait to the end of the patch
         (default value is WAIT_TIMEOUT)
        :type timeout: int
        :param max_threads: max number of threads to open during waiting
        (default value is DEFAULT_MAX_THREADS)
        :type: max_threads: int
        """
        pods = self.get(name=name,
                        namespace=namespace)
        self.client_app.patch_namespaced_daemon_set(name=name,
                                                    namespace=namespace,
                                                    body=body)
        logger.info("Patched daemon set {name} from namespace {namespace}"
                    "".format(name=name,
                              namespace=namespace))
        if wait:
            self.wait_for_daemon_set_to_patch(name=name,
                                              pods=pods,
                                              namespace=namespace,
                                              timeout=timeout,
                                              max_threads=max_threads)

    @k8s_exceptions
    def get(self,
            name,
            namespace=DEFAULT_NAMESPACE,
            dict_output=False):
        """
        Return daemon set obj or dictionary
        :param name: daemon set name
        :type name: str
        :param namespace: the namespace of the daemon set
        (default value is 'default')
        :type namespace: str
        :param dict_output: to return dictionary instead of obj
        :type dict_output: bool
        :return: the daemon set obj/dictionary
        :rtype: Union[V1DaemonSet,dictionary]
        """
        daemon_set = self.client_app.read_namespaced_daemon_set(
            name=name,
            namespace=namespace)
        logger.info("Got deployment {name} from {namespace} namespace".format(
            name=name,
            namespace=namespace))

        # convert the obj to dict if required
        if dict_output:
            daemon_set = convert_obj_to_dict(daemon_set)
        else:
            daemon_set.metadata.resource_version = ''

        return daemon_set

    @k8s_exceptions
    def list(self,
             namespace=DEFAULT_NAMESPACE,
             all_namespaces=False,
             dict_output=False,
             field_selector=""):
        """
        Return list of daemon set objects/dictionaries
        :param namespace: the namespace of the daemon set
        (default value is 'default')
        :type namespace: str
        :param all_namespaces: to get the list from all the namespaces
        :type all_namespaces: bool
        :param dict_output: to get the elements of the list dictionaries
        instead of objects
        :type dict_output: bool
        :param field_selector: to filter the list to specific daemon sets
        :type field_selector: str
        :return: list of daemon sets
        :rtype: list
        """
        if all_namespaces:
            daemon_sets_list = \
                self.client_app.list_daemon_set_for_all_namespaces().items
            logger.info("Got the daemon sets list from all the namespaces")
        else:
            daemon_sets_list = self.client_app.list_namespaced_daemon_set(
                namespace=namespace).items
            logger.info("Got the daemon sets list from {namespace} namespace"
                        "".format(namespace=namespace))

        if field_selector:
            daemon_sets_list = field_filter(obj_list=daemon_sets_list,
                                            field_selector=field_selector)

        # convert the list to list of dicts if required
        if dict_output:
            daemon_sets_list = [convert_obj_to_dict(deployment)
                                for deployment in daemon_sets_list]
        else:
            for deployment in daemon_sets_list:
                deployment.metadata.resource_version = ''

        return daemon_sets_list

    def list_names(self,
                   namespace=DEFAULT_NAMESPACE,
                   all_namespaces=False,
                   field_selector=""):
        return [daemon_set.metadata.name
                for daemon_set in self.list(namespace=namespace,
                                            all_namespaces=all_namespaces,
                                            field_selector=field_selector)]

    def get_pods(self,
                 name,
                 namespace=DEFAULT_NAMESPACE,
                 dict_output=False):
        """
        Return the pods of the daemon set
        :param name: the name of the daemon set
        :type name: str
        :param namespace: the namespace of the daemon set
        (default value is 'default')
        :type namespace: str
        :param dict_output: to get the elements of the list dictionaries
        instead of objects
        :type dict_output: bool
        :return: the pods of the daemon set
        :rtype: list
        """
        pods_list = self.pod.list(
            namespace=namespace,
            field_selector="metadata.owner_references[0].kind==DaemonSet, "
                           "metadata.owner_references[0].name=={name}"
                           "".format(name=name),
            dict_output=dict_output
        )
        return pods_list

    @k8s_exceptions
    def events(self,
               name,
               namespace=DEFAULT_NAMESPACE,
               only_messages=False):
        """
        Return the list of the events of a specific daemon set
        :param name: the name of the daemon set
        :type name: str
        :param namespace: the namespace of the daemon set
        (default value is 'default')
        :type namespace: str
        :param only_messages: to get only the events messages instead of
        getting all the objects
        :return: the list of the events
        :rtype: list
        """
        daemon_set_uid = self.get(name=name,
                                  namespace=namespace).metadata.uid
        events = self.client_app.list_namespaced_event(
            namespace=namespace,
            field_selector="involvedObject.uid=={daemon_set_uid}".format(
                daemon_set_uid=daemon_set_uid
            )
        ).items
        logger.info("Got the events of daemon set {name} from namespace "
                    "{namespace}".format(name=name,
                                         namespace=namespace)
                    )
        if only_messages:
            events = [event["message"] for event in events
                      if event.get("message") is not None]
        return events


if __name__ == "__main__":
    pass
