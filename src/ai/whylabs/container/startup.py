import uvicorn
import logging
import faster_fifo_reduction
from uvicorn.logging import DefaultFormatter


def init_logging() -> None:
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    formatter = DefaultFormatter("%(levelprefix)s [%(asctime)s %(name)s] %(message)s", datefmt="%d-%m-%Y-%H:%M:%S")
    ch.setFormatter(formatter)

    logging.basicConfig(handlers=[ch])
    logging.root.setLevel(logging.DEBUG)


from .main import actor

if __name__ == "__main__":
    init_logging()
    logger = logging.getLogger('startup')
    actor.start()

    config = uvicorn.Config("ai.whylabs.container.main:app", host="0.0.0.0", port=8000, reload=True, log_level="info")
    server = uvicorn.Server(config)
    logger.info("Visit http://localhost:8000/docs")
    server.run()
    actor.join()
