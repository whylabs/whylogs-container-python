import logging

from fastapi import Body, Depends, FastAPI, HTTPException, Request, status
from faster_fifo import Queue

from ...util.time import current_time_ms
from ..actor.profile_actor import DebugMessage, ProfileActor, PublishMessage, RawLogMessage
from .auth import Auth
from .requests import LogMultiple, LogRequest

logger = logging.getLogger("routes")


app = FastAPI()
auth = Auth()
_DEFAULT_QUEUE_SIZE_BYTES = 1000 * 1000 * 1000
actor = ProfileActor(Queue(_DEFAULT_QUEUE_SIZE_BYTES))


ex = LogRequest(
    datasetId="hi",
    multiple=LogMultiple(columns=["a", "b", "c"], data=[["foo", 1, 2.0], ["bar", 4, 3.0]]),
    single=None,
).json()


@app.post("/log", dependencies=[Depends(auth.api_key_auth)])
async def log(_raw_request: Request) -> None:
    b: bytes = await _raw_request.body()
    await actor.send(RawLogMessage(request=b, request_time=current_time_ms()))


@app.post("/log_docs", dependencies=[Depends(auth.api_key_auth)])
async def log_docs(body: LogRequest) -> None:
    """
    This endpoint is a bit silly. I can't find an easy way of manually controlling endpoint docs in the generated swagger
    and I can't declare /log's body as LogRequest because that has the side effect of also performing serde automatically,
    which performs far too poorly to keep in the server process. This endpoint is just for swagger docs on the body type
    for convenience but it shouldn't actually be used.
    """
    raise Exception("use the /log endpoint instead. This only exists for auto generated swagger documentation on the body type")


@app.post("/publish", dependencies=[Depends(auth.api_key_auth)])
async def publish_profiles() -> None:
    await actor.send(PublishMessage())


@app.post("/health", dependencies=[Depends(auth.api_key_auth)])
async def health() -> None:
    # TODO implement this in such a way that forces anything that will go wrong to go wrong now, like an incorrectly
    # built container config
    pass


@app.post("/logDebugInfo", dependencies=[Depends(auth.api_key_auth)])
async def log_debug_info() -> None:
    await actor.send(DebugMessage())


@app.on_event("shutdown")
async def shutdown() -> None:
    logger.info("Shutting down web server")
    await actor.shutdown()
