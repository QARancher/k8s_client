import pytest


@pytest.mark.usefixtures("clean_all")
class BaseTest(object):
    pass

