import logging
import os

"""
from uvicorn.config import LOGGING_CONFIG

LOGGING_CONFIG["formatters"]["default"]["fmt"] = \
    '%(asctime)s [%(name)s] %(levelprefix)s %(client_addr)s - "%(request_line)s" %(status_code)s'
LOGGING_CONFIG["formatters"]["access"]["fmt"] = \
    '%(asctime)s [%(name)s] %(levelprefix)s %(client_addr)s - "%(request_line)s" %(status_code)s'
"""

logging.getLogger("cassandra").setLevel(logging.INFO)
logging.getLogger("gmqtt").setLevel(logging.WARNING)
logging.getLogger("kafka").setLevel(logging.INFO)

CQL_ALLOW_MNG = "CQLENG_ALLOW_SCHEMA_MANAGEMENT"

# allows the application to modify the schema in cassandra if it's outdated
if os.getenv(CQL_ALLOW_MNG) is None:
    os.environ[CQL_ALLOW_MNG] = "1"

from dinofw.utils import environ

# keep this import; even though unused, uvicorn needs it, otherwise it will not start
from dinofw.restful import app  # noqa

environ.env.node = "app"
