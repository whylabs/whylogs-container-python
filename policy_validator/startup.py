import asyncio
import logging

import click
import uvicorn
from uvicorn.logging import DefaultFormatter

_logger = logging.getLogger("startup")


def init_logging() -> None:
    log_level = logging.INFO

    ch = logging.StreamHandler()
    ch.setLevel(log_level)

    # Define a formatter with colored output and specific format
    formatter = DefaultFormatter("%(levelprefix)s [%(asctime)s %(name)s] %(message)s", datefmt="%d-%m-%Y-%H:%M:%S")
    ch.setFormatter(formatter)

    # Attach the handler to the root logger
    logging.basicConfig(level=log_level)
    root_logger = logging.getLogger()
    root_logger.handlers = []  # Clear existing handlers
    root_logger.addHandler(ch)  # Add your handler
    root_logger.setLevel(log_level)

    # Specific loggers can be set to different levels if needed
    logging.getLogger("whylogs.core.column_profile").setLevel(log_level)
    logging.getLogger("whylogs.core.schema").setLevel(log_level)

    for logger_name in logging.Logger.manager.loggerDict:
        logger = logging.getLogger(logger_name)
        logger.setLevel(log_level)


def start(port: int = 8000) -> None:
    from policy_validator.routes import init_routes

    _event_loop = asyncio.new_event_loop()
    app = init_routes()
    config = uvicorn.Config(app, host="0.0.0.0", port=port, log_level=logging.INFO)
    server = uvicorn.Server(config)
    _logger.info(f"Visit http://localhost:{port}/docs")
    _event_loop.run_until_complete(server.serve())


@click.command()
@click.option("-p", "--port", type=int, required=False, default=8000, help="The port to run the server on", envvar="PORT")
def cli(port: int) -> None:
    init_logging()
    start()


if __name__ == "__main__":
    cli()
