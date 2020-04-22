import json
import logging
from functools import wraps
from time import perf_counter

from kubernetes.client.rest import ApiException

from consts import WAIT_TIMEOUT
from exceptions import InvalidFieldSelector, K8sException

logger = logging.getLogger(__name__)


def k8s_exceptions(func):
    """
    wrapper for k8s api exception, it catches ApiException and raises
    K8sException based on the reason in the API.
    """

    def func_wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except ApiException as e:
            error = json.loads(e.body)

            raise K8sException(
                message=f"Executed {func.__name__!r} \nFailed with error:\n"
                        f" {error.get('message')}",
                reason=error.get('reason'))

    return func_wrapper


def wait_for(func):
    """wrapper for function to wait for timeout after certain time.
    please use with function that return true / false."""
    @wraps(func)
    def wrapper_wait(*args, **kwargs):
        start_time = perf_counter()
        while True and WAIT_TIMEOUT <= perf_counter() + start_time:
            try:
                value = func(*args, **kwargs)
                if value:
                    return False
            except:
                return True
    return wrapper_wait


def underscore_to_uppercase(dict_to_edit):
    """
    This function convert underscore convention to uppercase convention
    input: dictionary
    output: dictionary with uppercase convention
    """
    if not isinstance(dict_to_edit, (dict, list)):
        return dict_to_edit
    if isinstance(dict_to_edit, list):
        return [value for value in
                (underscore_to_uppercase(value) for value in dict_to_edit) if
                value]
    for key in list(dict_to_edit.keys()):
        new_key = ''.join(word.capitalize() for word in key.split('_'))
        new_key = new_key[0].lower() + new_key[1:]
        dict_to_edit[new_key] = dict_to_edit.pop(key)
    return {key: value for key, value in
            ((key, underscore_to_uppercase(value)) for key, value in
             dict_to_edit.items()) if value}


def convert_obj_to_dict(obj):
    obj = obj.to_dict()
    obj.get("metadata", {}).pop("resource_version")
    obj = underscore_to_uppercase(obj)
    return obj


def split_list_to_chunks(list_to_slice, number_of_chunks):
    chunk = {}
    for pod_offset in range(0, len(list_to_slice), number_of_chunks):
        for chunk_num in range(number_of_chunks):
            if len(list_to_slice) > pod_offset + chunk_num:
                chunk.setdefault(chunk_num, []).append(
                    list_to_slice[pod_offset + chunk_num])
            else:
                return chunk.values()
    return chunk.values()


def field_filter(obj_list, field_selector):
    return [obj for obj in obj_list if
            selector(obj=obj, field_selector=field_selector)]


def selector(obj, field_selector):
    return all(condition(obj=obj, field_selector=field.strip()) for field in
               field_selector.split(","))


def condition(obj, field_selector):
    obj_selector = obj
    if "!=" in field_selector:
        attrs, val = field_selector.split("!=")
    elif "==" in field_selector:
        attrs, val = field_selector.split("==")
    elif "=" in field_selector:
        attrs, val = field_selector.split("=")
    else:
        raise InvalidFieldSelector()
    for attr in attrs.split("."):
        index = None
        if '[' in attr and attr[-1] == ']':
            try:
                index = int(attr[attr.index('[') + 1:-1])
                attr = attr.split('[', 1)[0]
            except ValueError:
                raise InvalidFieldSelector()
        try:
            obj_selector = getattr(obj_selector, attr)
        except AttributeError:
            return False
        try:
            if isinstance(obj_selector, list) and index is not None:
                obj_selector = obj_selector[index]
        except IndexError:
            return False
    if "!=" in field_selector:
        return obj_selector != str(val)
    else:
        return obj_selector == str(val)


if __name__ == "__main__":
    pass
