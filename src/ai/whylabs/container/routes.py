from fastapi import FastAPI, Request, Body
from faster_fifo import Queue

from ..actor.profile_actor import DebugMessage, ProfileActor, PublishMessage, RawLogMessage
from .requests import LogRequest, LogMultiple
import logging

logger = logging.getLogger('routes')

app = FastAPI()
_DEFAULT_QUEUE_SIZE_BYTES = 1000 * 1000 * 1000
actor = ProfileActor(Queue(_DEFAULT_QUEUE_SIZE_BYTES))


ex = LogRequest(
    datasetId='hi',
    multiple=LogMultiple(
        columns=['a', 'b', 'c'],
        data=[
            ['foo',1,2.0],
            ['bar',4,3.0]
        ]
    ),
    single=None,
).json()
@app.post("/log")
async def log(_raw_request: Request) -> None:
    b: bytes = await _raw_request.body()
    # TODO assign a dataset timestamp here asap before queueing it up for profiling
    await actor.send(RawLogMessage(b))

@app.post("/log_docs")
async def log_docs(body: LogRequest) -> None:
    """
    This endpoint is a bit silly. I can't find an easy way of manually controlling endpoint docs in the generated swagger
    and I can't declare /log's body as LogRequest because that has the side effect of also performing serde automatically,
    which performs far too poorly to keep in the server process. This endpoint is just for swagger docs on the body type
    for convenience but it shouldn't actually be used.
    """
    raise Exception('use the /log endpoint instead. This only exists for auto generated swagger documentation on the body type')

@app.post("/publish")
async def publish_profiles() -> None:
    await actor.send(PublishMessage())


@app.post("/logDebugInfo")
async def log_debug_info() -> None:
    await actor.send(DebugMessage())

@app.on_event("shutdown")
async def shutdown() -> None:
    logger.info('Shutting down web server')    
    await actor.shutdown()


