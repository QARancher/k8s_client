class K8sImage(object):
    def __init__(self, image_name="", version="", repository=None):
        super(K8sImage, self).__init__()
        self.image_name = image_name
        self.version = version or "latest"
        self.repository = repository

    @property
    def image(self):
        return f"{self.image_name}:{self.version}"

    @property
    def full_image(self):
        if self.repository:
            return f"{self.repository}{self.image_name}:{self.version}"
        else:
            return self.image


