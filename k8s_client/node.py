import logging
import paramiko

from k8s_client.consts import KEY_PATH, USER_NAME
from k8s_client.exceptions import K8sException
from k8s_client.utils import k8s_exceptions, convert_obj_to_dict, field_filter

logger = logging.getLogger(__name__)


class NodeClient(object):
    def __init__(self,
                 client_core):
        self.client_core = client_core

    def execute(self,
                name,
                command):
        """
        Execute command on node
        :param name: the name of the node
        :type name: str
        :param command: the command to run on the node
        :type command: str
        :return: the response
        :rtype: str
        """
        external_ip = self.get_external_ip(name=name)
        if external_ip is not None and KEY_PATH is not None:
            # open session with the node
            session = paramiko.SSHClient()
            session.set_missing_host_key_policy(policy=paramiko.AutoAddPolicy())
            session.connect(hostname=external_ip,
                            username=USER_NAME,
                            key_filename=KEY_PATH)
            _, stdout, stderr = session.exec_command(command)
            logger.info(f"Executed command {command} on "
                        "node {name}")
            # close the session
            session.close()
            return stdout.read()
        elif external_ip is None:
            raise K8sException(message=f"Could not find an external ip of "
                                       "node {name}")

    def get(self,
            name,
            dict_output=False):
        """
        Return node obj or dictionary
        :param name: node name
        :type name: str
        :param dict_output: to return dictionary instead of obj
        :type dict_output: bool
        :return: the node obj/dictionary
        :rtype: Union[V1Node,dictionary]
        """
        nodes_list = self.list(dict_output=dict_output,
                               field_selector=f"metadata.name=={name}")
        logger.info(f"Got node {name}")
        return nodes_list[0]

    def get_address(self,
                    name,
                    kind):
        addresses = self.get(name=name).get("status", {}).get("addresses", [])
        return next((address for address in addresses
                     if address.get("type") == kind),
                    None)

    def get_internal_ip(self,
                        name):
        logger.info(f"Get the internal ip of node {name}")
        return self.get_address(name=name,
                                kind="InternalIP")

    def get_external_ip(self,
                        name):
        logger.info(f"Get the external ip of node {name}")
        return self.get_address(name=name,
                                kind="ExternalIP")

    @k8s_exceptions
    def list(self,
             dict_output=False,
             field_selector=""):
        """
        Return list of nodes objects/dictionaries
        :param dict_output: to get the elements of the list dictionaries
        instead of objects
        :type dict_output: bool
        :param field_selector: to filter the list to specific nodes
        :type field_selector: str
        :return: list of nodes
        :rtype: list
        """
        nodes_list = self.client_core.list_node().items
        logger.info("Got nodes")

        if field_selector:
            nodes_list = field_filter(obj_list=nodes_list,
                                      field_selector=field_selector)

        # convert the list to list of dicts if required
        if dict_output:
            nodes_list = [convert_obj_to_dict(namespace)
                          for namespace in nodes_list]
        else:
            for node in nodes_list:
                node.metadata.resource_version = ''

        return nodes_list

    def list_names(self,
                   field_selector=""):
        return [node.metadata.name
                for node in self.list(field_selector=field_selector)]

    @k8s_exceptions
    def events(self,
               name,
               only_messages=False):
        """
        Return the list of the events of a specific node
        :param name: the name of the node
        :type name: str
        :param only_messages: to get only the events messages instead of
        getting all the objects
        :return: the list of the events
        :rtype: list
        """
        node_id = self.get(name=name, ).metadata.uid
        events = self.client_core.list_event_for_all_namespaces(
            field_selector=f"involvedObject.uid=={node_id}").items
        logger.info(f"Got the events of node {name}")
        if only_messages:
            events = [event["message"] for event in events
                      if event.get("message") is not None]
        return events

    @k8s_exceptions
    def patch(self,
              name,
              body):
        """
        Patch node
        :param name: the name of the node
        :type name: str
        :param body: the diff body to patch
        :type body: dictionary
        """
        logger.info(f"Patch node {name}")
        self.client_core.patch_node(name=name,
                                    body=body)

    def add_label(self,
                  name,
                  label):
        """
        Add label to a specific node
        :param name: the node name
        :type name: str
        :param label: the label to add to the node
        :type label: str
        """
        logger.info(f"Add label {label} to node {name}")
        node = self.get(name=name)
        if hasattr(node.metadata, 'labels'):
            patch_dict = {"metadata": {"labels": node.metadata.labels}}
        else:
            patch_dict = {"metadata": {"labels": {}}}
        label_key, label_val = label.split("=", 1)
        patch_dict["metadata"]["labels"][label_key] = label_val

        self.patch(name=name,
                   body=patch_dict)


if __name__ == "__main__":
    pass
