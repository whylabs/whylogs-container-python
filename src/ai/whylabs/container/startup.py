import logging
import asyncio

import uvicorn
from uvicorn.logging import DefaultFormatter

from .routes import actor


def init_logging() -> None:
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    formatter = DefaultFormatter("%(levelprefix)s [%(asctime)s %(name)s] %(message)s", datefmt="%d-%m-%Y-%H:%M:%S")
    ch.setFormatter(formatter)

    logging.getLogger('whylogs.core.column_profile').setLevel(logging.INFO)
    logging.getLogger('whylogs.core.schema').setLevel(logging.INFO)

    logging.basicConfig(handlers=[ch])
    logging.root.setLevel(logging.DEBUG)


if __name__ == "__main__":
    init_logging()
    logger = logging.getLogger('startup')
    actor.daemon = True
    actor.start()
    actor.join(0.1)

    config = uvicorn.Config("ai.whylabs.container.routes:app", host="0.0.0.0", port=8000, reload=True, log_level="info")
    server = uvicorn.Server(config)
    logger.info("Visit http://localhost:8000/docs")
    server.run()