from fastapi import FastAPI, Request
from pydantic import BaseModel
from fastapi import Body
from faster_fifo import Queue

from ..actor.profile_actor import CSVMessage, ProfileActor, DebugMessage


app = FastAPI()
_DEFAULT_QUEUE_SIZE_BYTES = 1000 * 1000 * 1000
actor = ProfileActor(Queue(_DEFAULT_QUEUE_SIZE_BYTES))


@app.post("/csv")
async def profile_csv(csv: bytes = Body(media_type="application/octet-stream")) -> None:
    await actor.send(CSVMessage(csv))


@app.post("/logDebugInfo")
async def log_debug_info() -> None:
    await actor.send(DebugMessage())
