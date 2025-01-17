from pathlib import Path
from typing import Final

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sentry_sdk.integrations.asgi import SentryAsgiMiddleware

from dinofw.router import delete
from dinofw.router import get
from dinofw.router import post
from dinofw.router import put
from dinofw.utils import environ
from dinofw.utils.custom_logging import CustomizeLogger
from dinofw.utils.config import ConfigKeys

API_VERSION: Final = "v1"


def create_app():
    api = FastAPI()

    config_path = Path(__file__).with_name("logging_config.json")
    custom_logger = CustomizeLogger.make_logger(config_path)
    api.logger = custom_logger

    api.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    api.add_middleware(SentryAsgiMiddleware)

    api.include_router(
        post.router,
        prefix=f"/{API_VERSION}",
        tags=["POST"],
        responses={404: {"description": "Not found"}},
    )
    api.include_router(
        get.router,
        prefix=f"/{API_VERSION}",
        tags=["GET"],
        responses={404: {"description": "Not found"}},
    )
    api.include_router(
        put.router,
        prefix=f"/{API_VERSION}",
        tags=["PUT"],
        responses={404: {"description": "Not found"}},
    )
    api.include_router(
        delete.router,
        prefix=f"/{API_VERSION}",
        tags=["DELETE"],
        responses={404: {"description": "Not found"}},
    )

    return api


app = create_app()


@app.on_event("startup")
async def startup():
    await environ.startup()
    await environ.env.client_publisher.setup()
    environ.env.server_publisher.setup()


@app.on_event("shutdown")
async def shutdown_event():
    await environ.env.client_publisher.stop()
