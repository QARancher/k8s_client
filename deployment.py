import logging
import threading
from kubernetes.client import V1Deployment


from utils import (
    convert_obj_to_dict,
    split_list_to_chunks,
    field_filter, k8s_exceptions
)
from consts import (
    DEFAULT_NAMESPACE,
    WAIT_TIMEOUT,
    REPLICAS_THRESHOLD,
    DEFAULT_MAX_THREADS
)

from exceptions import K8sInvalidResourceBody

logger = logging.getLogger(__name__)


class DeploymentClient(object):

    def __init__(self,
                 client_app,
                 pod):
        self.client_app = client_app
        self.pod = pod

    def finished_to_create_ready_replicas(self,
                                          name,
                                          namespace):
        """
        Return if the replicas of a deployment are created
        :param name: the name of the deployment
        :type name: str
        :param namespace: the namespace of the deployment
        (default value is 'default')
        :type namespace: str
        :return: created or not (True/False)
        :rtype: bool
        """
        deployment = self.get(name=name,
                              namespace=namespace)
        return deployment.spec.replicas == deployment.status.replicas

    @staticmethod
    def wait_for_pods_thread(pod_wait_func,
                             pod_kwargs_list):
        """
        Thread function that runs the input function with the input vars
        :param pod_wait_func: function to run in thread
        :type pod_wait_func: function
        :param pod_kwargs_list: list of kwargs
        :type pod_kwargs_list: list
        """
        for kwargs in pod_kwargs_list:
            pod_wait_func(**kwargs)

    def wait_for_pods_creation_thread_manager(self,
                                              pods,
                                              namespace=DEFAULT_NAMESPACE,
                                              timeout=WAIT_TIMEOUT,
                                              max_threads=DEFAULT_MAX_THREADS):
        """
        Wait until the all pods are running
        (if there are enough pods opens threads)
        :param pods:  the deployment's pods
        :type pods: list
        :param namespace: the namespace of the deployment
        :type namespace: str
        :param timeout: timeout to wait to the creation
        (default value is WAIT_TIMEOUT)
        :type timeout: int
        :param max_threads: max number of threads to open during waiting
        (default value is DEFAULT_MAX_THREADS)
        :type max_threads: int
        """
        if len(pods) > REPLICAS_THRESHOLD:
            threads = []
            kwargs_list = []
            for pod in pods:
                kwargs = {
                    "pod_name": pod.metadata.name,
                    "pod_id": pod.metadata.uid,
                    "containers_counter": len(pod.spec.containers),
                    "namespace": namespace,
                    "timeout": timeout
                          }
                kwargs_list.append(kwargs)
            for pods_kwargs in split_list_to_chunks(
                    list_to_slice=kwargs_list,
                    number_of_chunks=max_threads):
                threads.append(
                    threading.Thread(
                        target=self.wait_for_pods_thread,
                        args=(self.pod.wait_for_containers_to_run, pods_kwargs)
                        )
                    )
                threads[len(threads) - 1].start()
            for thread in threads:
                thread.join()
        else:
            for pod in pods:
                self.pod.wait_for_containers_to_run(
                    pod_name=pod.metadata.name,
                    pod_id=pod.metadata.uid,
                    containers_counter=len(pod.spec.containers),
                    namespace=namespace,
                    timeout=timeout
                )

    def wait_for_deployment_to_run(self,
                                   deployment_name,
                                   namespace=DEFAULT_NAMESPACE,
                                   timeout=WAIT_TIMEOUT,
                                   max_threads=DEFAULT_MAX_THREADS):
        """
        Wait until the deployment is running (including their pods...)
        :param deployment_name: the name of the deployment
        :type deployment_name: str
        :param namespace: the namespace of the deployment
        (default value is 'default')
        :type namespace: str
        :param timeout: timeout to wait to the creation
        (default value is WAIT_TIMEOUT)
        :type timeout: int
        :param max_threads: max number of threads to open during waiting
        (default value is DEFAULT_MAX_THREADS)
        :type max_threads: int
        """
        if not self.finished_to_create_ready_replicas(name=deployment_name,
                                                      namespace=namespace):
            return False
        self.wait_for_pods_creation_thread_manager(pods=self.get_pods(
                                                        name=deployment_name,
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
        Create deployment
        :param body: deployment's body
        :type body: dictionary or V1Deployment
        :param namespace: the namespace to create the deployment in if there is
        no namespace in the yaml (default value is 'default')
        :type namespace: str
        :param wait: to wait until the creation is over (default value is True)
        :type wait: bool
        :param timeout: timeout to wait to the creation
        (default value is WAIT_TIMEOUT)
        :type timeout: int
        :param max_threads: max number of threads to open during waiting
        (default value is DEFAULT_MAX_THREADS)
        :type max_threads: int
        :return: deployment name
        :rtype: str
        """
        # check the type of the body and that it contains name
        # and raise exception if not
        try:
            if isinstance(body, V1Deployment):
                deployment_name = body.metadata.name
                if hasattr(body, "metadata") and \
                        hasattr(body.metadata, "namespace"):
                    namespace = body.metadata.namespace
            elif isinstance(body, dict):
                deployment_name = body["metadata"]["name"]
                namespace = body.get("metadata", {}).get("namespace", namespace)
            else:
                raise K8sInvalidResourceBody()
        except (KeyError, AttributeError):
            raise K8sInvalidResourceBody()

        # create the deployment from the body
        self.client_app.create_namespaced_deployment(
            body=body,
            namespace=namespace)
        logger.info("Created the deployment {deployment_name} in {namespace} "
                    "namespace".format(deployment_name=deployment_name,
                                       namespace=namespace)
                    )

        # wait to the deployment to run
        if wait:
            self.wait_for_deployment_to_run(deployment_name=deployment_name,
                                            namespace=namespace,
                                            timeout=timeout,
                                            max_threads=max_threads)
        return deployment_name

    def wait_for_pods_to_be_deleted_thread_manager(
            self,
            pods,
            namespace=DEFAULT_NAMESPACE,
            timeout=WAIT_TIMEOUT,
            max_threads=DEFAULT_MAX_THREADS):
        """
        Wait until the deployment's pods are deleted
        :param pods: the deployment's pods
        :type pods: list
        :param namespace: the namespace of the deployment
        (default value is 'default')
        :type namespace: str
        :param timeout: timeout to wait to the deletion
        (default value is WAIT_TIMEOUT)
        :type timeout: int
        :param max_threads: max number of threads to open during waiting
        (default value is DEFAULT_MAX_THREADS)
        :type max_threads: int
        """
        if len(pods) > REPLICAS_THRESHOLD:
            threads = []
            kwargs_list = []
            for pod in pods:
                kwargs = {
                    "pod_name": pod.metadata.name,
                    "namespace": namespace,
                    "timeout": timeout
                          }
                kwargs_list.append(kwargs)
            for pods_kwargs in split_list_to_chunks(
                    list_to_slice=kwargs_list,
                    number_of_chunks=max_threads):
                threads.append(
                    threading.Thread(
                        target=self.wait_for_pods_thread,
                        args=(self.pod.wait_for_pod_to_be_deleted, pods_kwargs)
                        )
                    )
                threads[len(threads) - 1].start()
            for thread in threads:
                thread.join()
        else:
            for pod in pods:
                self.pod.wait_for_pod_to_be_deleted(
                    pod_name=pod.metadata.name,
                    namespace=namespace,
                    timeout=timeout
                )
        logger.info("Finished waiting before the timeout {timeout}"
                    "".format(timeout=timeout))
        return True

    @k8s_exceptions
    def delete(self,
               name,
               namespace=DEFAULT_NAMESPACE,
               wait=False,
               timeout=WAIT_TIMEOUT,
               max_threads=DEFAULT_MAX_THREADS):
        """
        Delete deployment
        :param name: deployment's name
        :type name: str
        :param namespace: the namespace to delete the deployment from
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
        self.client_app.delete_namespaced_deployment(name=name,
                                                     namespace=namespace)
        logger.info("Deleted deployment {name} from {namespace} namespace"
                    "".format(name=name,
                              namespace=namespace))

        # wait to the pods to be deleted
        if wait:
            logger.info("Wait to {deployment_name} to be deleted with "
                        "{timeout} timeout".format(deployment_name=name,
                                                   timeout=timeout)
                        )
            self.wait_for_pods_to_be_deleted_thread_manager(
                pods=pods,
                namespace=namespace,
                timeout=timeout,
                max_threads=max_threads)


    def wait_for_deployment_to_patch(self,
                                     name,
                                     pods,
                                     namespace=DEFAULT_NAMESPACE,
                                     timeout=WAIT_TIMEOUT,
                                     max_threads=DEFAULT_MAX_THREADS):
        """
        Wait until the deployment's pods are patched
        :param name: the name of the deployment
        :type name: str
        :param pods: the deployment's pods
        :type pods: list
        :param namespace: the namespace of the deployment
        (default value is 'default')
        :type namespace: str
        :param timeout: timeout to wait to the patch
        (default value is WAIT_TIMEOUT)
        :type timeout: int
        :param max_threads: max number of threads to open during waiting
        (default value is DEFAULT_MAX_THREADS)
        :type max_threads: int
        """
        self.wait_for_pods_to_be_deleted_thread_manager(
            pods=pods,
            namespace=namespace,
            timeout=timeout,
            max_threads=max_threads)
        self.wait_for_deployment_to_run(deployment_name=name,
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
        Patch deployment
        :param name: the name of the deployment
        :type name: str
        :param body: the diff body to patch
        :type body: dictionary
        :param namespace: the namespace of the deployment
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
        self.client_app.patch_namespaced_deployment(name=name,
                                                    namespace=namespace,
                                                    body=body)
        logger.info("Patched deployment {name} from namespace {namespace}"
                    "".format(name=name,
                              namespace=namespace))
        if wait:
            self.wait_for_deployment_to_patch(name=name,
                                              pods=pods,
                                              namespace=namespace,
                                              timeout=timeout,
                                              max_threads=max_threads)

    def wait_for_deployment_to_scale_up(self,
                                        name,
                                        pods,
                                        namespace=DEFAULT_NAMESPACE,
                                        timeout=WAIT_TIMEOUT,
                                        max_threads=DEFAULT_MAX_THREADS):
        """
        Wait until the deployment is scaled up
        :param name: the name of the deployment
        :type name: str
        :param pods: the deployment's pods
        :type pods: list
        :param namespace: the namespace of the deployment
        (default value is 'default')
        :type namespace: str
        :param timeout: timeout to wait to the scale up
        (default value is WAIT_TIMEOUT)
        :type timeout: int
        :param max_threads: max number of threads to open during waiting
        (default value is DEFAULT_MAX_THREADS)
        :type max_threads: int
        """
        if not self.finished_to_create_ready_replicas(name=name,
                                                      namespace=namespace):
            return False

        new_pods = [pod for pod in self.get_pods(name=name,
                                                 namespace=namespace)
                    if pod not in pods]
        self.wait_for_pods_creation_thread_manager(pods=new_pods,
                                                   namespace=namespace,
                                                   timeout=timeout,
                                                   max_threads=max_threads)
        logger.info("Finished waiting before the timeout {timeout}"
                    "".format(timeout=timeout))
        return True

    def wait_for_deployment_to_scale_down(self,
                                          name,
                                          new_size,
                                          namespace=DEFAULT_NAMESPACE,
                                          timeout=WAIT_TIMEOUT):
        """
        Wait until the deployment is scaled down
        :param name: the name of the deployment
        :type name: str
        :param new_size: the new amount of replicas
        :type new_size: int
        :param namespace: the namespace of the deployment
        (default value is 'default')
        :type namespace: str
        :param timeout: timeout to wait to the scale down
        (default value is WAIT_TIMEOUT)
        """
        is_scaled_down = len(self.get_pods(name=name,
                                           namespace=namespace)) == new_size
        if is_scaled_down:
            logger.info("Finished waiting before the timeout {timeout}"
                        "".format(timeout=timeout)
                        )
        return is_scaled_down

    def scale(self,
              name,
              new_size,
              namespace=DEFAULT_NAMESPACE,
              wait=True,
              timeout=WAIT_TIMEOUT,
              max_threads=DEFAULT_MAX_THREADS):
        """
        Scale deployment
        :param name: the name of the deployment
        :type name: str
        :param new_size: the new number of replicas for the deployment
        :type new_size: int
        :param namespace: the namespace of the deployment
        (default value is 'default')
        :type namespace: str
        :param wait: to wait until the scale up is over
        (default value is True)
        :type wait: bool
        :param timeout: timeout to wait to the end of the scale
         (default value is WAIT_TIMEOUT)
        :type timeout: int
        :param max_threads: max number of threads to open during waiting
        (default value is DEFAULT_MAX_THREADS)
        :type: max_threads: int
        """
        pods = self.get_pods(name=name,
                             namespace=namespace)
        body = {"spec": {"replicas": new_size}}
        self.patch(name=name,
                   body=body,
                   namespace=namespace,
                   wait=False,
                   timeout=timeout,
                   max_threads=max_threads)
        logger.info("Scaled deployment {name} from namespace {namespace}"
                    "".format(name=name,
                              namespace=namespace))
        if wait:
            if new_size > len(pods):
                self.wait_for_deployment_to_scale_up(name=name,
                                                     pods=pods,
                                                     namespace=namespace,
                                                     timeout=timeout,
                                                     max_threads=max_threads)
            elif new_size < len(pods):
                self.wait_for_deployment_to_scale_down(name=name,
                                                       new_size=new_size,
                                                       namespace=namespace,
                                                       timeout=timeout)

    def scale_down_up(self,
                      name,
                      namespace=DEFAULT_NAMESPACE,
                      wait=True,
                      timeout=WAIT_TIMEOUT,
                      max_threads=DEFAULT_MAX_THREADS):
        """
        Scale down and up deployment
        :param name: the name of the deployment
        :type name: str
        :param namespace: the namespace of the deployment
        (default value is 'default')
        :type namespace: str
        :param wait: to wait until the scale up is over
        (default value is True)
        :type wait: bool
        :param timeout: timeout to wait to the end of the scale
         (default value is WAIT_TIMEOUT)
        :type timeout: int
        :param max_threads: max number of threads to open during waiting
        (default value is DEFAULT_MAX_THREADS)
        :type: max_threads: int
        """
        replicas = self.get(name=name,
                            namespace=namespace).spec.replicas
        self.scale(name=name,
                   new_size=0,
                   namespace=namespace,
                   wait=wait,
                   timeout=timeout,
                   max_threads=max_threads)
        self.scale(name=name,
                   new_size=replicas,
                   namespace=namespace,
                   wait=wait,
                   timeout=timeout,
                   max_threads=max_threads)

    @k8s_exceptions
    def get(self,
            name,
            namespace=DEFAULT_NAMESPACE,
            dict_output=False):
        """
        Return deployment obj or dictionary
        :param name: deployment name
        :type name: str
        :param namespace: the namespace of the deployment
        (default value is 'default')
        :type namespace: str
        :param dict_output: to return dictionary instead of obj
        :type dict_output: bool
        :return: the deployment obj/dictionary
        :rtype: Union[V1Deployment,dictionary]
        """
        deployment = self.client_app.read_namespaced_deployment(
            name=name,
            namespace=namespace)
        logger.info("Got deployment {name} from {namespace} namespace".format(
            name=name,
            namespace=namespace))

        # convert the obj to dict if required
        if dict_output:
            deployment = convert_obj_to_dict(deployment)
        else:
            deployment.metadata.resource_version = ''

        return deployment

    @k8s_exceptions
    def list(self,
             namespace=DEFAULT_NAMESPACE,
             all_namespaces=False,
             dict_output=False,
             field_selector=""):
        """
        Return list of deployments objects/dictionaries
        :param namespace: the namespace of the deployment
        (default value is 'default')
        :type namespace: str
        :param all_namespaces: to get the list from all the namespaces
        :type all_namespaces: bool
        :param dict_output: to get the elements of the list dictionaries
        instead of objects
        :type dict_output: bool
        :param field_selector: to filter the list to specific deployments
        :type field_selector: str
        :return: list of deployments
        :rtype: list
        """
        if all_namespaces:
            deployments_list = \
                self.client_app.list_deployment_for_all_namespaces().items
            logger.info("Got the deployments list from all the namespaces")
        else:
            deployments_list = self.client_app.list_namespaced_deployment(
                namespace=namespace).items
            logger.info("Got the deployments list from {namespace} namespace"
                        "".format(namespace=namespace))

        if field_selector:
            deployments_list = field_filter(obj_list=deployments_list,
                                            field_selector=field_selector)

        # convert the list to list of dicts if required
        if dict_output:
            deployments_list = [convert_obj_to_dict(deployment)
                                for deployment in deployments_list]
        else:
            for deployment in deployments_list:
                deployment.metadata.resource_version = ''

        return deployments_list

    def list_names(self,
                   namespace=DEFAULT_NAMESPACE,
                   all_namespaces=False,
                   field_selector=""):
        return [deployment.metadata.name
                for deployment in self.list(namespace=namespace,
                                            all_namespaces=all_namespaces,
                                            field_selector=field_selector)]

    @k8s_exceptions
    def get_pods(self,
                 name,
                 namespace=DEFAULT_NAMESPACE,
                 dict_output=False):
        """
        Return the pods of the deployment
        :param name: the name of the deployment
        :type name: str
        :param namespace: the namespace of the deployment
        (default value is 'default')
        :type namespace: str
        :param dict_output: to get the elements of the list dictionaries
        instead of objects
        :type dict_output: bool
        :return: the pods of the deployment
        :rtype: list
        """
        deploy_replica_sets = self.client_app.list_namespaced_replica_set(
            namespace=namespace
        ).items
        deploy_replica_sets = field_filter(
            obj_list=deploy_replica_sets,
            field_selector="metadata.owner_references[0].kind==Deployment, "
                           "metadata.owner_references[0].name=={name}"
                           "".format(name=name))
        pods_list = []
        for deploy_replica_set in deploy_replica_sets:
            pods_list.extend(self.pod.list(
                namespace=namespace,
                field_selector="metadata.owner_references[0].kind==ReplicaSet,"
                               "metadata.owner_references[0].name=={name}"
                               "".format(
                                    name=deploy_replica_set.metadata.name),
                dict_output=dict_output
                )
            )
        return pods_list

    @k8s_exceptions
    def events(self,
               name,
               namespace=DEFAULT_NAMESPACE,
               only_messages=False):
        """
        Return the list of the events of a specific deployment
        :param name: the name of the deployment
        :type name: str
        :param namespace: the namespace of the deployment
        (default value is 'default')
        :type namespace: str
        :param only_messages: to get only the events messages instead of
        getting all the objects
        :return: the list of the events
        :rtype: list
        """
        deployment_uid = self.get(name=name,
                                  namespace=namespace).metadata.uid
        events = self.client_app.list_namespaced_event(
            namespace=namespace,
            field_selector="involvedObject.uid=={deployment_uid}".format(
                deployment_uid=deployment_uid
            )
        ).items
        logger.info("Got the events of deployment {name} from namespace "
                    "{namespace}".format(name=name,
                                         namespace=namespace)
                    )
        if only_messages:
            events = [event["message"] for event in events
                      if event.get("message") is not None]
        return events


if __name__ == "__main__":
    pass
