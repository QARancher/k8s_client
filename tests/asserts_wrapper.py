def assert_not_none(actual_result, message=""):
    if not message:
        message = "{actual_result} resulted with None".format(
            actual_result=actual_result)
    assert actual_result, message


def assert_equal(actual_result, expected_result, message=""):
    if not message:
        message = "{actual_result} is not equal to expected" \
                  " result {expected_result}".format(
            actual_result=actual_result, expected_result=expected_result)
    assert actual_result == expected_result, message


def assert_in_list(list, wanted_element, message=""):
    if not message:
        message = "Failed to find '{wanted_element}' in list {list}".format(
            wanted_element=wanted_element, list=list)
    assert wanted_element in list, message


def assert_not_in_list(list, unwanted_element, message=""):
    if not message:
        message = "'{unwanted_element}' found in list {list} \n " \
                  "although it shouldn't be".format(
            unwanted_element=unwanted_element, list=list)
    assert unwanted_element not in list, message
