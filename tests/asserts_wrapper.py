def assert_not_none(actual_result, message=""):
    if not message:
        message = f"{actual_result} resulted with None"
    assert actual_result, message


def assert_equal(actual_result, expected_result, message=""):
    if not message:
        message = f"{actual_result} is not equal to expected " \
                  f"result {expected_result}"
    assert actual_result == expected_result, message


def assert_in_list(searched_list, wanted_element, message=""):
    if not message:
        message = f"Failed to find '{wanted_element}' in list {searched_list}"
    assert wanted_element in searched_list, message


def assert_not_in_list(searched_list, unwanted_element, message=""):
    if not message:
        message = f"'{unwanted_element}' found in list {searched_list} \n " \
                  f"although it should not be"
    assert unwanted_element not in searched_list, message


def assert_of_type(wanted_type, wanted_object, message=""):
    if not message:
        message = f"{wanted_object} is not of type: {wanted_type}"
    assert isinstance(wanted_object, wanted_type), message
