from k8s_resource import K8sResource, unpack


class K8sService(K8sResource):
    def __init__(self,
                 name="",
                 api_version="v1",
                 body=None,
                 svc_type="ClusterIP",
                 selector=None,
                 ports=None):
        super(K8sService, self).__init__(kind="Service",
                                         name=name,
                                         api_version=api_version,
                                         body=body)
        self.type = svc_type
        self.selector = selector or {}
        self.ports = ports or []

    @property
    def type(self):
        return self["spec"].get("type")

    @type.setter
    def type(self,
             type):
        self["spec"]["type"] = type

    @property
    def ports(self):
        return self["spec"].get("ports")

    @ports.setter
    def ports(self,
              ports):
        self["spec"]["ports"] = ports

    def add_ports(self,
                  ports_list):
        """
        Add port to the service
        :param ports_list: list of tuples
            [(<port>, <protocol>, <targetPort>, <name>),
             (<port>, <protocol>), (<port>,),  ...]
        :type ports_list: list
        """
        ports_list = ports_list or []
        for port_info in ports_list:
            port_obj = {}
            port_obj["port"], other_data = unpack(seq=port_info)
            if len(other_data):
                port_obj["protocol"], other_data = unpack(seq=other_data)
            if len(other_data):
                port_obj["targetPort"], other_data = unpack(seq=other_data)
            if len(other_data):
                port_obj["name"], other_data = unpack(other_data)
            self["spec"].setdefault("ports", []).append(port_obj)

    @property
    def annotations(self):
        return self["metadata"].get("annotations")

    @annotations.setter
    def annotations(self,
                    annotations):
        self["metadata"]["annotations"] = annotations

    @property
    def labels(self):
        return self["metadata"].get("labels")

    @labels.setter
    def labels(self,
               labels):
        self["metadata"]["labels"] = labels
        self["spec"]["selector"] = {
            "matchLabels": labels
        }


if __name__ == "__main__":
    pass
