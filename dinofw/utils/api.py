import inspect

from fastapi import HTTPException
from fastapi import status
from loguru import logger

from dinofw.utils import environ
from dinofw.utils.config import ErrorCodes


# dependency
async def get_db():
    async with environ.env.SessionLocal() as db:
        yield db


def log_error_and_raise_unknown(exc_info, e):
    func_name = inspect.currentframe().f_back.f_code.co_name
    logger.error(f"{func_name}: {str(e)}")
    logger.exception(e)
    environ.env.capture_exception(exc_info)
    raise HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail=f"{ErrorCodes.UNKNOWN_ERROR}: {str(e)}",
    )


def log_error_and_raise_known(error_code, exc_info, e):
    details = f"{error_code}: {e.message}"
    logger.error(details)
    environ.env.capture_exception(exc_info)
    raise HTTPException(
        status_code=error_code,
        detail=details,
    )
