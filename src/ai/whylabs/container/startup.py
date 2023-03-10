import uvicorn
from uvicorn.logging import DefaultFormatter
import logging
from ..actor.actor import start_actor

# Make sure logger initialization is the first thing that happens
def init_logging() -> None:
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    formatter = DefaultFormatter("%(levelprefix)s [%(asctime)s %(name)s] %(message)s", datefmt="%d-%m-%Y-%H:%M:%S")
    ch.setFormatter(formatter)

    logging.getLogger("whylogs.core.column_profile").setLevel(logging.INFO)
    logging.getLogger("whylogs.core.schema").setLevel(logging.INFO)

    logging.basicConfig(handlers=[ch])
    logging.root.setLevel(logging.DEBUG)


init_logging()


from ..actor.actor import Actor
from .routes import actor


def update_pid(act: Actor) -> None:
    logger.info(f"Profiler process pid: {actor.pid}")
    with open("/tmp/profiling_pid", "w") as f:
        f.write(str(act.pid))


if __name__ == "__main__":
    logger = logging.getLogger("startup")

    start_actor(actor)
    update_pid(actor)

    config = uvicorn.Config(
        "ai.whylabs.container.routes:app", host="0.0.0.0", port=8000, reload=True, log_level=logging.WARN
    )
    server = uvicorn.Server(config)
    logger.info("Visit http://localhost:8000/docs")
    server.run()
