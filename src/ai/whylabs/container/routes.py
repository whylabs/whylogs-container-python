from fastapi import Body, FastAPI, Request
from faster_fifo import Queue

from ..actor.profile_actor import DebugMessage, ProfileActor, PublishMessage, LogMessage, CloseMessage
import logging
from .requests import LogRequest

logger = logging.getLogger('routes')

app = FastAPI()
_DEFAULT_QUEUE_SIZE_BYTES = 1000 * 1000 * 1000
actor = ProfileActor(Queue(_DEFAULT_QUEUE_SIZE_BYTES))


@app.post("/log")
async def log(_raw_request: Request) -> None:
    csv: bytes = await _raw_request.body()
    req = LogRequest.parse_raw(csv)
    await actor.send(LogMessage(req))

@app.post("/csv")
async def log_csv(request: Request) -> None:
    csv: bytes = await request.body()
    # await actor.send(LogMessage(request))

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


