import logging
from dinofw import environ

# keep this import; even though unused, uvicorn needs it, otherwise it will not start
from dinofw.restful import app

logging.getLogger("kafka").setLevel(logging.INFO)

environ.env.node = "app"