import json
import logging
import os
import sys
from typing import Literal, Optional, cast

import click
import uvicorn
from uvicorn.logging import DefaultFormatter

from whylogs_container.whylabs.container.log_util import add_message_filter
from whylogs_container.whylabs.container.server_config import ServerConfig, get_server_config, set_server_config


# Make sure logger initialization is the first thing that happens
def init_logging() -> None:
    env = get_server_config()
    log_level = env.log_level

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

    # OTEL complains whenever you pass it a null value but it properly filters them out and proably
    # fastesr and more comprehensively than if we were to do it.
    add_message_filter(
        filter_prefix="Invalid type NoneType for attribute",
        logger_name="opentelemetry.attributes",
        log_level=logging.WARN,
    )


class Server:
    def __init__(self, setup_logging: bool = True, config: Optional[ServerConfig] = None) -> None:
        if config:
            set_server_config(config)

        server_config = get_server_config()

        if not server_config.llm_container:
            from whylogs_container.whylabs.container.startup_util import fake_llm_deps

            fake_llm_deps()

        if server_config.mac_dev_mode:
            var = "OBJC_DISABLE_INITIALIZE_FORK_SAFETY"
            if var not in os.environ:
                raise ValueError(
                    f"""
{var} must be set to YES manually in mac_dev_mode before starting the server. For example:

    $ {var}=YES make server-llm
""".strip()
                )
            import multiprocessing

            multiprocessing.set_start_method("fork")

        if setup_logging:
            init_logging()

        # most imports are done here to avoid log initialization races in the dependencies that are imported and to allow the above
        # dependency mocking to happen when we're not running the llm container.
        import asyncio

        import whylogs as why
        from whylogs_container.whylabs.container.config import ConfigActor, ConfigInstance, ConfigManager, ConfigState
        from whylogs_container.whylabs.container.routes_base import init_routes_base
        from whylogs_container.whylabs.container.routes_profiling import init_routes_profiling
        from whylogs_container.whylabs.container.version import version
        from whylogs_container.whylabs.logger.logger import create_logger

        # forcing local avoid writing config to disk
        why.init(force_local=True, allow_local=True, whylabs_api_key=server_config.whylabs_api_key)
        self._logger = logging.getLogger("startup")

        ConfigState().get_container_config()

        self._event_loop = asyncio.new_event_loop()
        self._logger.info(f"Container version {version}")
        self._logger.info(f"Parent process pid {os.getpid()}")
        self._config_manager = ConfigManager()
        self._config_manager.start()
        self._config_instance = cast(ConfigInstance, self._config_manager.ConfigInstance())  # pyright: ignore[reportUnknownMemberType, reportAttributeAccessIssue]

        self._config_actor = ConfigActor(self._config_instance)

        if server_config.disable_profiling:
            self._whylogs_logger = None
        else:
            self._whylogs_logger = create_logger(self._config_instance)

        if server_config.llm_container:
            from whylogs_container.whylabs.llm_validation.validator import SynchronousValidator, ValidationActor

            self._validator: Optional[SynchronousValidator] = ValidationActor(self._config_instance, self._whylogs_logger, self._event_loop)
            if server_config.aws_sqs_url and server_config.aws_sqs_dlq_url and server_config.llm_container:
                from whylogs_container.whylabs.container.aws.sqs_poller import SQSPoller

                self._aws_sqs_poller = SQSPoller(
                    server_config.aws_sqs_url, server_config.aws_sqs_dlq_url, self._validator, self._config_instance
                )
            else:
                self._aws_sqs_poller = None
        else:
            self._validator = None
            self._aws_sqs_poller = None

        self.app = init_routes_base(self._whylogs_logger, self._validator, self._config_actor)

        if self._whylogs_logger:
            init_routes_profiling(self.app, self._whylogs_logger, self._validator, self._config_actor)

        if server_config.llm_container:
            from whylogs_container.whylabs.container.routes_llm import init_llm_routes
            from whylogs_container.whylabs.container.routes_llm_lightllm import init_lightllm_routes

            init_llm_routes(self.app, self._whylogs_logger, self._validator, self._config_instance, self._aws_sqs_poller)
            init_lightllm_routes(self.app, self._whylogs_logger, self._validator, self._config_instance)

            # Import here because it will trigger a langkit import
            from whylogs_container.whylabs.container.responses import AddResponseHeaderMiddleWare

            self.app.add_middleware(AddResponseHeaderMiddleWare)

        self.app.add_event_handler("shutdown", self.server_shutdown)  # pyright: ignore[reportUnknownMemberType]
        self.app.add_event_handler("startup", self.server_start)  # pyright: ignore[reportUnknownMemberType]

    def server_start(self):
        if self._aws_sqs_poller:
            self._aws_sqs_poller.start()

    def server_shutdown(self):
        if self._aws_sqs_poller:
            self._aws_sqs_poller.stop()

        if self._validator:
            self._validator.close_validator()

        if self._whylogs_logger:
            self._whylogs_logger.close()
        self._config_actor.close_process()

    def start(self, port=8000) -> None:
        """
        Start all of the sub processes and the uvicorn server.
        """
        self._config_actor.start_process()
        # This initializes all of the llm metrics and policies, which has the effect of loading a lot of machine learning libraries into
        # memory. Subprocesses share this memory rather than each process reinitializing it.
        # This should be called before the actors start or the process won't exit gracefully if there is a config failure.
        try:
            self._config_actor.refresh()
            pass
        except Exception as e:
            self._logger.error(f"Failed to start server: {e}")
            self._config_actor.close_process()
            sys.exit(1)

        if self._whylogs_logger:
            self._whylogs_logger.start()

        if self._validator:
            try:
                self._validator.start_validator()
                if get_server_config().pre_warm_cache:
                    self._validator.warm_caches()
            except Exception as e:
                self._logger.error(f"Failed to start server: {e}")
                self._config_actor.close_process()
                if self._whylogs_logger:
                    self._whylogs_logger.close()
                sys.exit(1)

        config = uvicorn.Config(self.app, host="0.0.0.0", port=port, log_level=logging.INFO)
        server = uvicorn.Server(config)
        self._logger.info(f"Visit http://localhost:{port}/docs")
        self._event_loop.run_until_complete(server.serve())


Mode = Literal["server", "openapi-gen"]


@click.command()
@click.option("-p", "--port", type=int, required=False, default=8000, help="The port to run the server on", envvar="PORT")
@click.option(
    "-f",
    "--openapi-file",
    type=str,
    required=False,
    default="./openapi.json",
    help="A file path to dump the openapi spec to, if running in that mode",
    envvar="OPENAPI_FILE",
)
@click.option(
    "--mode",
    type=click.Choice(["server", "openapi-gen"]),
    required=False,
    help="Whether to run the server or generate the openapi spec",
    default="server",
    envvar="MODE",
)
def cli(openapi_file: str, port: int, mode: Mode) -> None:
    if mode == "openapi-gen":
        from fastapi.openapi.utils import get_openapi

        server = Server()

        spec = get_openapi(
            title=server.app.title,
            version=server.app.version,
            openapi_version=server.app.openapi_version,
            description=server.app.description,
            routes=server.app.routes,
        )

        with open(openapi_file, "w") as file:
            json.dump(spec, file, indent=2)

    else:
        server = Server()
        server.start(port)


if __name__ == "__main__":
    cli()
