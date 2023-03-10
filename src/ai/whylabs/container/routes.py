import logging

from fastapi import Depends, FastAPI, Request
from faster_fifo import Queue

from .config import ContainerConfig
from ...util.time import current_time_ms
from ..actor.profile_actor import DebugMessage, ProfileActor, PublishMessage, RawLogMessage, RawLogEmbeddingsMessage
from .auth import Auth
from .requests import LogRequest

logger = logging.getLogger("routes")


app = FastAPI()
auth = Auth()
_DEFAULT_QUEUE_SIZE_BYTES = 1000 * 1000 * 1000
actor = ProfileActor(Queue(_DEFAULT_QUEUE_SIZE_BYTES))
config = ContainerConfig()
auth_dependencies = [Depends(auth.api_key_auth)] if not config.auth_disabled() else []


@app.post("/log", dependencies=auth_dependencies)
async def log(_raw_request: Request) -> None:
    b: bytes = await _raw_request.body()
    actor.send(RawLogMessage(request=b, request_time=current_time_ms()))


@app.post("/log-embeddings", dependencies=auth_dependencies)
async def log_embeddings(_raw_request: Request) -> None:
    b: bytes = await _raw_request.body()
    actor.send(RawLogEmbeddingsMessage(request=b, request_time=current_time_ms()))


@app.post("/log-pubsub", dependencies=auth_dependencies)
async def log_pubsub(_raw_request: Request) -> None:
    b: bytes = await _raw_request.body()
    actor.send(RawLogEmbeddingsMessage(request=b, request_time=current_time_ms()))


@app.post("/log_docs", dependencies=auth_dependencies)
async def log_docs(body: LogRequest, _raw_request: Request) -> None:
    """
    This endpoint is a bit silly. I can't find an easy way of manually controlling endpoint docs in the generated swagger
    and I can't declare /log's body as LogRequest because that has the side effect of also performing serde automatically,
    which performs far too poorly to keep in the server process. This endpoint is just for swagger docs on the body type
    for convenience but it shouldn't actually be used.
    """
    await log(_raw_request)


@app.post("/publish", dependencies=auth_dependencies)
async def publish_profiles() -> None:
    actor.send(PublishMessage())


@app.post("/health", dependencies=auth_dependencies)
async def health() -> None:
    # TODO implement this in such a way that forces anything that will go wrong to go wrong now, like an incorrectly
    # built container config
    pass


@app.post("/logDebugInfo", dependencies=auth_dependencies)
async def log_debug_info() -> None:
    actor.send(DebugMessage())


@app.on_event("shutdown")
async def shutdown() -> None:
    logger.info("Shutting down web server")
    actor.shutdown()
