import sys
import time
import traceback
from functools import wraps

from fastapi import HTTPException
from fastapi.responses import JSONResponse

from dinofw.utils import environ


# used for non-async methods
def time_method(_logger, prefix: str, threshold_ms: int = 10):
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


def timeit(_logger, method: str, tag: str = None, threshold_ms: int = 10, only_log: bool = False):
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
                        _logger.debug(f"{method} {tag or ''}... {the_time:.2f}ms {relevant_args}")

                    # don't always need to send telemetry to upstreams stats collector
                    if only_log:
                        return

                    if environ.env.stats is not None:
                        stats_tag = method.lower().replace("(", "").replace(")", "")
                        if tag is not None:
                            tag_cleaned = tag.lstrip("/").replace("/", ".").replace("{", "").replace("}", "")
                            stats_tag = f"{stats_tag}.{tag_cleaned}"

                        _logger.debug(f"calling statsd with tag '{stats_tag}' and time {the_time:.2f}ms")
                        environ.env.stats.timing('api.' + stats_tag, the_time)

        return decorator
    return factory


def wrap_exception():
    """
    wrap exceptions so we can customize the error response usually sent by
    FastAPI that only contain 'detail' and not option for an error code
    """
    def factory(view_func):
        @wraps(view_func)
        async def decorator(*args, **kwargs):
            try:
                return await view_func(*args, **kwargs)
            except HTTPException as e:
                # uvicorn forbids non-standard http response codes, so only use 400/500 for errors
                http_code = 400
                if e.status_code == 500:
                    http_code = 500

                return JSONResponse(status_code=http_code, content={
                    "code": e.status_code,
                    "detail": e.detail
                })
        return decorator
    return factory
