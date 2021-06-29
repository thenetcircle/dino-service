import sys
import time
import traceback
from functools import wraps

from fastapi import HTTPException
from fastapi.responses import JSONResponse

from dinofw.utils import environ


def time_method(_logger, prefix: str):
    def factory(view_func):
        @wraps(view_func)
        def decorator(*args, **kwargs):
            before = time.time()
            try:
                return view_func(*args, **kwargs)
            finally:
                the_time = (time.time() - before) * 1000
                if the_time > 10:
                    _logger.debug(f"{prefix} took {the_time:.2f}ms")
        return decorator
    return factory


def timeit(_logger, method: str, tag: str):
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
                _logger.error(tag + '... FAILED')
                environ.env.capture_exception(sys.exc_info())
                raise e
            finally:
                if not failed:
                    the_time = (time.time()-before) * 1000

                    stats_tag = tag.lstrip("/").replace("/", ".").replace("{", "").replace("}", "")
                    stats_tag = f"{method.lower()}.{stats_tag}"

                    if the_time > 10:
                        relevant_args = {
                            key: value for
                            key, value in kwargs.items()
                            if kwargs is not None and key not in {"db"}
                        }
                        _logger.debug(f"{method} {tag}... {the_time:.2f}ms {relevant_args}")

                    if environ.env.stats is not None:
                        _logger.debug(f"calling statsd with tag '{stats_tag}' and time {the_time:.2f}ms")
                        environ.env.stats.timing('api.' + stats_tag, the_time)
        return decorator
    return factory


def wrap_exception():
    def factory(view_func):
        @wraps(view_func)
        def decorator(*args, **kwargs):
            try:
                return view_func(*args, **kwargs)
            except HTTPException as e:
                return JSONResponse(status_code=e.status_code, content={
                    "detail": e.detail
                })
        return decorator
    return factory
