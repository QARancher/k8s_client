from helpers.k8s_resource import K8sResource, get_current_timestamp


class K8sPod(K8sResource):
    def __init__(self,
                 name="",
                 api_version="v1",
                 body=None,
                 restart_policy="Never"):
        super(K8sPod, self).__init__(kind="Pod",
                                     name=name,
                                     api_version=api_version,
                                     body=body)
        self.restart_policy = restart_policy
        self.generation = 1
        self.creation_timestamp = get_current_timestamp()
        self.containers = []
        self.image_pull_secrets = []
        self.volumes = []
        self.node_selector = {}
        self.tolerations = []

    @property
    def containers(self):
        return self["spec"].get("containers")

    @containers.setter
    def containers(self,
                   containers):
        self["spec"]["containers"] = containers

    def add_container(self,
                      container_obj):
        self["spec"].setdefault("containers", []).append(container_obj)

    @property
    def image_pull_secrets(self):
        return self["spec"].get("imagePullSecrets")

    @image_pull_secrets.setter
    def image_pull_secrets(self,
                           image_pull_secrets):
        self["spec"]["imagePullSecrets"] = image_pull_secrets

    @property
    def node_selector(self):
        return self["spec"].get("nodeSelector")

    @node_selector.setter
    def node_selector(self,
                      node_selector):
        self["spec"]["nodeSelector"] = node_selector

    @property
    def tolerations(self):
        return self["spec"].get("tolerations")

    @tolerations.setter
    def tolerations(self,
                    tolerations):
        self["spec"]["tolerations"] = tolerations

    def add_tolerations_list(self,
                             tolerations_list):
        for toleration in tolerations_list:
            self["spec"].setdefault("tolerations", []).append(
                {
                    "effect": "NoSchedule",
                    "key": toleration,
                    "operator": "Exists"
                }
            )

    @property
    def restart_policy(self):
        return self["spec"].get("restartPolicy")

    @restart_policy.setter
    def restart_policy(self,
                       restart_policy):
        self["spec"]["restartPolicy"] = restart_policy

    def add_image_pull_secrets(self,
                               image_pull_secrets):
        for secret in image_pull_secrets:
            self["spec"].setdefault("imagePullSecrets", []).append(
                {
                    "name": secret
                })

    @property
    def volumes(self):
        return self["spec"].get("volumes")

    @volumes.setter
    def volumes(self,
                volumes):
        self["spec"]["volumes"] = volumes

    def add_volumes_dict(self,
                         volumes_dict):
        for vol_name, host_vol in volumes_dict.iteritems():
            self["spec"].setdefault("volumes", []).append(
                {
                    "name": vol_name,
                    "hostPath":
                        {
                            "path": host_vol
                        }
                })


if __name__ == "__main__":
    pass
