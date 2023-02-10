from fastapi import Body, FastAPI, Request
from faster_fifo import Queue

from ..actor.profile_actor import DebugMessage, ProfileActor, PublishMessage, LogMessage, RawLogMessage
import logging
from .requests import LogRequest

logger = logging.getLogger('routes')

app = FastAPI()
_DEFAULT_QUEUE_SIZE_BYTES = 1000 * 1000 * 1000
actor = ProfileActor(Queue(_DEFAULT_QUEUE_SIZE_BYTES))


@app.post("/log")
async def log(_raw_request: Request) -> None:
    # TODO uh oh, using the built in serde with pydantic in fastapi is incredibly, incredibly slow. Moving it all out
    # and manually doing it in the profiling process dramatically increases throughput from 700tps -> 3500tps
    b: bytes = await _raw_request.body()
    # TODO assign a dataset timestamp here asap before queueing it up for profiling
    # req = LogRequest.parse_raw(csv)
    await actor.send(RawLogMessage(b))


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


