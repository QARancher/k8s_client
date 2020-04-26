import os


DEFAULT_NAMESPACE = "default"
WAIT_TIMEOUT = 60
COMPLETE_STATE = "Completed"
ERROR_STATE = "Error"
AUTHENTICATION_EXCEPTION = "unauthorized"
PULLING_EXCEPTION = "pull access denied"
PULLING_FAIL = "Failed to pull"
CREATED_SUCCESSFULLY = "Started container"
REPLICAS_THRESHOLD = 1
DEFAULT_MAX_THREADS = 20
KEY_PATH = os.environ.get("KEY_PATH")
KUBECONFIG_PATH = os.environ.get("KUBECONFIG_PATH", "~/.kube/config")
USER_NAME = os.environ.get("USER_NAME", "")