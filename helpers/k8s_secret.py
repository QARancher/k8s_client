from helpers.k8s_resource import K8sResource, get_current_timestamp


class K8sSecret(K8sResource):
    def __init__(self,
                 name="",
                 api_version="v1",
                 body=None,
                 data=None):
        data = data or {}
        self._kind = "Secret"
        super(K8sSecret, self).__init__(kind=self._kind,
                                        name=name,
                                        api_version=api_version,
                                        body=body)
        if not self.data and not data:
            self.data = data
        self.creation_timestamp = get_current_timestamp()

    @property
    def resource_version(self):
        return self["metadata"].get("resourceVersion")

    @resource_version.setter
    def resource_version(self,
                         resource_version):
        self["metadata"]["resourceVersion"] = resource_version

    @property
    def data(self):
        return self.get("data")

    @data.setter
    def data(self,
             data):
        self["data"] = data

    @property
    def self_link(self):
        return self["metadata"].get("selfLink")

    @self_link.setter
    def self_link(self,
                  self_link):
        self["metadata"]["selfLink"] = self_link

    @property
    def type(self):
        return self.get("type")

    @type.setter
    def type(self,
             type):
        self["type"] = type


if __name__ == "__main__":
    pass
