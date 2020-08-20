import logging
from dinofw import client_environ

# keep this import; even though unused, uvicorn needs it, otherwise it will not start
from dinofw.client import app

logging.getLogger("kafka").setLevel(logging.INFO)

client_environ.env.node = "client"
