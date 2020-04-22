from datetime import datetime

from exceptions import K8sInvalidResourceBody


def get_current_timestamp():
    # generates RFC-3339 time stamp
    return datetime.utcnow().isoformat("T") + "Z"


def unpack(seq, number_to_unpack_first=1):
    it = iter(seq)
    for x in range(number_to_unpack_first):
        yield next(it, None)
    yield tuple(it)


def create_container(image, container_name, privileged=None, as_user=None,
                     ports_list=None, env_dict=None, volumes_dict=None,
                     resources_limits_dict=None, resources_requests_dict=None,
                     image_pull_policy="", command="", args=""):
    container_obj = {"image": image, "name": container_name, "env": [],
        "volumeMounts": [], "ports": [], "securityContext": {},
        "imagePullPolicy": image_pull_policy or "Always",
        "resources": {"requests": {}, "limits": {}}}
    if command:
        container_obj["command"] = command.split(" ")
    if args:
        container_obj["args"] = args.split(" ")
    if privileged is not None:
        container_obj["securityContext"]["privileged"] = privileged
    if as_user is not None:
        container_obj["securityContext"]["runAsUser"] = as_user
    if ports_list is not None:
        # [(<port>, <protocol>), (<port>,), ...]
        for port_info in ports_list:
            port_obj = {}
            port_obj["containerPort"], other_data = unpack(seq=port_info)
            if len(other_data):
                port_obj["protocol"], other_data = unpack(seq=other_data)

            container_obj["ports"].append(port_obj)
    if env_dict is not None:
        for env, val in env_dict.iteritems():
            container_obj["env"].append({"name": env, "value": val})
    if volumes_dict is not None:
        for host_vol, mount_vol in volumes_dict.iteritems():
            readonly = False
            if str(mount_vol).endswith(":ro"):
                readonly = True
                mount_vol = mount_vol[:-3]

            container_obj["volumeMounts"].append(
                {"name": mount_vol, "mountPath": mount_vol,
                 "readOnly": readonly})
    if resources_requests_dict is not None:
        container_obj['resources']['requests'] = resources_requests_dict
    if resources_limits_dict is not None:
        container_obj['resources']['limits'] = resources_limits_dict
    return container_obj


class K8sResource(dict):
    def __init__(self, kind="", name="", api_version="", body=None):
        body = body or {}
        super(K8sResource, self).__init__()
        self["metadata"] = {}
        self["spec"] = {}
        self.update(body)
        if not self.get("metadata", {}).get("name"):
            if name:
                self["metadata"]["name"] = name
            else:
                raise K8sInvalidResourceBody("Did not satisfy a name to the "
                                             "resource")
        if not self.get("kind"):
            if kind:
                self["kind"] = kind
            else:
                raise K8sInvalidResourceBody("Did not satisfy kind to the "
                                             "resource")
        if not self.get("apiVersion"):
            if kind:
                self["apiVersion"] = api_version
            else:
                raise K8sInvalidResourceBody("Did not satisfy an apiVersion "
                                             "to the resource")

    @property
    def body(self):
        return self

    @property
    def name(self):
        return self["metadata"].get("name")

    @name.setter
    def name(self, name):
        self["metadata"]["name"] = name

    @property
    def namespace(self):
        return self["metadata"].get("namespace")

    @namespace.setter
    def namespace(self, namespace):
        self["metadata"]["namespace"] = namespace

    @property
    def kind(self):
        return self["kind"]

    @kind.setter
    def kind(self, kind):
        self["kind"] = kind

    @property
    def api_version(self):
        return self["apiVersion"]

    @api_version.setter
    def api_version(self, api_version):
        self["apiVersion"] = api_version

    @property
    def generation(self):
        return self["metadata"].get("generation")

    @generation.setter
    def generation(self, generation):
        self["metadata"]["generation"] = generation

    @property
    def creation_timestamp(self):
        return self["metadata"].get("creationTimestamp")

    @creation_timestamp.setter
    def creation_timestamp(self, creation_timestamp):
        self["metadata"]["creationTimestamp"] = creation_timestamp

    @property
    def selector(self):
        return self["spec"].get("selector")

    @selector.setter
    def selector(self, selector):
        self["spec"]["selector"] = selector

    @property
    def labels(self):
        return self["metadata"].get("labels")

    @labels.setter
    def labels(self, labels):
        self["metadata"]["labels"] = labels

    def add_labels(self, labels):
        for name, label in labels.iteritems():
            self["metadata"].setdefault("labels", {})[name] = label


if __name__ == "__main__":
    pass
