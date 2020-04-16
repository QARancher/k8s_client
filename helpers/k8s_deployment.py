from helpers.k8s_daemonset import K8sDaemonSet


class K8sDeployment(K8sDaemonSet):
    def __init__(self,
                 name="",
                 api_version="apps/v1",
                 body=None,
                 replicas=1,
                 selector=None,
                 restart_policy="Always"):
        super(K8sDeployment, self).__init__(kind="Deployment",
                                            name=name,
                                            api_version=api_version,
                                            body=body,
                                            selector=selector,
                                            restart_policy=restart_policy)
        self.replicas = replicas

    @property
    def replicas(self):
        return self["spec"].get("replicas")

    @replicas.setter
    def replicas(self,
                 replicas):
        self["spec"]["replicas"] = replicas


if __name__ == "__main__":
    pass
