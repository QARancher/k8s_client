from k8s_resource import K8sResource
from new_k8s_cli.commons.kubectl_utils import KubectlUtils


class K8sDaemonSet(K8sResource):
    def __init__(self,
                 name="",
                 api_version="apps/v1",
                 body=None,
                 kind="Daemonset",
                 selector=None,
                 restart_policy="Always"):
        super(K8sDaemonSet, self).__init__(kind=kind,
                                           name=name,
                                           api_version=api_version,
                                           body=body)
        self.selector = selector or {}
        self.containers = []
        self.spec_volumes = []
        self.restart_policy = restart_policy

    @property
    def template(self):
        return self["spec"].setdefault("template",
                                       {"metadata": {}, "spec": {}}
                                       )

    @property
    def restart_policy(self):
        return self["spec"].get("template", {}).get("restart_policy")

    @restart_policy.setter
    def restart_policy(self,
                       restart_policy):
        self.template["restart_policy"] = restart_policy

    @property
    def labels(self):
        return self["metadata"].get("labels")

    @labels.setter
    def labels(self,
               labels):
        self["spec"]["labels"] = labels
        self["spec"]["selector"] = {
            "matchLabels": labels
        }
        self.template["metadata"]["labels"] = labels

    @property
    def image_pull_secrets(self):
        return self.template.get("spec", {}).get("imagePullSecrets")

    @image_pull_secrets.setter
    def image_pull_secrets(self,
                           image_pull_secrets):
        self.template["spec"]["imagePullSecrets"] = image_pull_secrets

    def add_image_pull_secrets(self,
                               image_pull_secret_list):
        for image_pull_secret in image_pull_secret_list:
            self.template["spec"].setdefault("imagePullSecrets", []).append({
                "name": image_pull_secret
            })

    @property
    def containers(self):
        return self.template.get("spec", {}).get(
            "containers")

    @containers.setter
    def containers(self,
                   containers):
        self.template["spec"]["containers"] = containers

    def add_containers(self,
                       container_obj):
        self.template["spec"].setdefault("containers", []).append(container_obj)

    @property
    def spec_volumes(self):
        return self.template.get("spec", {}).get("volumes")

    @spec_volumes.setter
    def spec_volumes(self,
                     spec_volumes):
        self.template["spec"]["volumes"] = spec_volumes

    @property
    def volumes(self):
        return self.template.get("volumes")

    @volumes.setter
    def volumes(self,
                volumes):
        self.template["volumes"] = volumes

    def add_volumes_dict(self,
                         volumes_dict):
        for host_vol, vol_name in volumes_dict:
            vol_name = KubectlUtils.get_k8s_valid_name(
                wanted_name=vol_name)
            self.template["template"].setdefault("volumes", []).append(
                {
                    "name": vol_name,
                    "hostPath": {
                        "path": str(host_vol)
                    }
                })


if __name__ == "__main__":
    pass
