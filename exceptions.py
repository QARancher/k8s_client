class K8sException(Exception):
    def __init__(self, message="", reason=None):
        if reason and 'NotFound' in reason:
            raise K8sNotFoundException(message=message)
        if reason and 'AlreadyExists':
            raise K8sAlreadyExists(message=message)
        super(K8sException, self).__init__(message)


class K8sInvalidResourceBody(K8sException):
    def __init__(self, message="Invalid resource's body."):
        super(K8sInvalidResourceBody, self).__init__(message)


class K8sAuthenticationException(K8sException):
    def __init__(self, message="Unauthorized to perform this action."):
        super(K8sAuthenticationException, self).__init__(message)


class K8sPullingException(K8sException):
    def __init__(self, message="Could not pull the required image."):
        super(K8sPullingException, self).__init__(message)


class InvalidFieldSelector(K8sException):
    def __init__(self, message="Invalid field selector."):
        super(InvalidFieldSelector, self).__init__(message)


class K8sNotFoundException(K8sException):
    def __init__(self, message="Could not find the required resource."):
        super(K8sNotFoundException, self).__init__(message)


class K8sAlreadyExists(K8sException):
    def __init__(self, message="Resource Already Exists"):
        super(K8sAlreadyExists, self).__init__(message)


class K8sRuntimeException(K8sException):
    def __init__(self, message="The resource got a running error."):
        super(K8sRuntimeException, self).__init__(message)


if __name__ == "__main__":
    pass
