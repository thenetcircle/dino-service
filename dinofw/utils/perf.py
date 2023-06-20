import sys
import time
import traceback
from functools import wraps

from dinofw.utils import environ


# used for non-async methods
def time_method(_logger, prefix: str, threshold_ms: int = 100):
    def factory(view_func):
        @wraps(view_func)
        def decorator(*args, **kwargs):
            before = time.time()
            try:
                return view_func(*args, **kwargs)
            finally:
                the_time = (time.time() - before) * 1000
                if the_time > threshold_ms:
                    _logger.debug(f"{prefix} took {the_time:.2f}ms")
        return decorator
    return factory


def timeit(_logger, method: str, tag: str = None, threshold_ms: int = 100, only_log: bool = False):
    def factory(view_func):
        @wraps(view_func)
        async def decorator(*args, **kwargs):
            failed = False
            before = time.time()
            try:
                return await view_func(*args, **kwargs)
            except Exception as e:
                failed = True
                _logger.exception(traceback.format_exc())
                if tag is not None:
                    _logger.error(tag + '... FAILED')
                environ.env.capture_exception(sys.exc_info())
                raise e
            finally:
                if not failed:
                    the_time = (time.time()-before) * 1000

                    if the_time > threshold_ms:
                        relevant_args = {
                            key: value for
                            key, value in kwargs.items()
                            if kwargs is not None and key not in {"db"}
                        }
                        _logger.info(f"{method} {tag or ''}... {the_time:.2f}ms {relevant_args}")

                    # don't always need to send telemetry to upstreams stats collector
                    if only_log:
                        return

                    if environ.env.stats is not None:
                        stats_tag = method.lower().replace("(", "").replace(")", "")
                        if tag is not None:
                            tag_cleaned = tag.lstrip("/").replace("/", ".").replace("{", "").replace("}", "")
                            stats_tag = f"{stats_tag}.{tag_cleaned}"

                        environ.env.stats.timing('api.' + stats_tag, the_time)

        return decorator
    return factory
