from functools import wraps
from time import perf_counter


def timeme(func):
    """Print the runtime of the decorated function"""
    @wraps(func)
    def wrapper_timer(*args, **kwargs):
        start_time = perf_counter()
        value = func(*args, **kwargs)
        run_time = perf_counter() - start_time
        print(f"Executed {func.__name__!r} in {run_time:.7f} s")
        return value, run_time
    return wrapper_timer
