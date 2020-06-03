import os
from gnenv import create_env

ENV_KEY_ENVIRONMENT = "DINO_ENVIRONMENT"

gn_environment = os.getenv(ENV_KEY_ENVIRONMENT)
env = create_env(gn_environment)
