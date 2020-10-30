import logging
from typing import Final

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from dinofw.router import delete
from dinofw.router import get
from dinofw.router import post
from dinofw.router import put
from dinofw.utils import environ

logger = logging.getLogger(__name__)

API_VERSION: Final = "v1"


def create_app():
    api = FastAPI()

    api.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
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
    await environ.env.client_publisher.setup()
    environ.env.server_publisher.setup()
