from k8s_resource import K8sResource, get_current_timestamp


class K8sNamespace(K8sResource):
    def __init__(self,
                 name="",
                 api_version="v1",
                 body=None):
        super(K8sNamespace, self).__init__(kind="Namespace",
                                           name=name,
                                           api_version=api_version,
                                           body=body)
        self.generation = 1
        self.creation_timestamp = get_current_timestamp()
        self.labels = {"name": name}


if __name__ == "__main__":
    pass
