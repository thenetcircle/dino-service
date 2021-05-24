import inspect
import logging
from loguru import logger as guru
import sys

from fastapi import HTTPException
from fastapi import status

from dinofw.utils import environ
from dinofw.utils.config import ErrorCodes

logger = logging.getLogger(__name__)


# dependency
def get_db():
    db = environ.env.SessionLocal()
    try:
        yield db
    finally:
        db.close()


def log_error_and_raise_unknown(exc_info, e):
    func_name = inspect.currentframe().f_back.f_code.co_name
    guru.error(f"{func_name}: {str(e)}")
    guru.exception(e)
    environ.env.capture_exception(sys.exc_info)
    raise HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail=f"{ErrorCodes.UNKNOWN_ERROR}: {str(e)}",
    )


def log_error_and_raise_known(error_code, exc_info, e):
    details = f"{error_code}: {e.message}"
    guru.error(details)
    environ.env.capture_exception(exc_info)
    raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST, detail=f"{error_code}: {e.message}",
    )
