# pyright: reportUnusedFunction=false
import base64
import logging
from typing import Optional

from fastapi import Depends, FastAPI
from fastapi.exceptions import HTTPException

from whylogs.api.logger.experimental.logger.actor.process_rolling_logger_messages import ProcessLoggerStatus
from whylogs_container.whylabs.container.config import SynchronousConfig

# TODO get this out of the routes file
from whylogs_container.whylabs.container.dependencies.api_key import api_key_dependency
from whylogs_container.whylabs.container.responses import (
    LoggerStatusResponse,
    StatusResponse,
)
from whylogs_container.whylabs.container.server_config import get_server_config
from whylogs_container.whylabs.container.version import version
from whylogs_container.whylabs.llm_validation.validator import ValidationActor
from whylogs_container.whylabs.logger.logger import ContainerProcessRollingLogger

logger = logging.getLogger("routes")


def init_routes_base(
    whylogs_logger: Optional[ContainerProcessRollingLogger],
    validator: Optional[ValidationActor],
    config_actor: SynchronousConfig,
) -> FastAPI:
    app = FastAPI()
    server_config = get_server_config()
    auth_dependencies = [Depends(api_key_dependency())] if not server_config.auth_disabled() else []

    @app.get("/health", operation_id="health", tags=["manage"])
    def health() -> None:
        logger.info("Checking server health")

        if whylogs_logger:
            whylogs_healthy = whylogs_logger.is_alive()
            if not whylogs_healthy:
                logger.error("WhyLogs process is not healthy")
                raise HTTPException(status_code=500, detail="WhyLogs logger is not healthy")

        if validator:
            validator_healthy = validator.is_alive()
            if not validator_healthy:
                logger.error("Validation process is not healthy")
                raise HTTPException(status_code=500, detail="Validation actor is not healthy")

        config_healthy = config_actor.is_process_alive()
        if not config_healthy:
            logger.error("Config process is not healthy")
            raise HTTPException(status_code=500, detail="Config actor is not healthy")

    @app.get("/healthz", operation_id="healthz", description="Duplicate of /health for compatibility", tags=["manage"])
    def healthz() -> None:
        return health()

    @app.get("/status", operation_id="status", dependencies=auth_dependencies, tags=["manage"])
    def status() -> StatusResponse:
        status = whylogs_logger.status() if whylogs_logger else ProcessLoggerStatus
        server_config = get_server_config()
        safe_config = server_config.get_values_safe()

        return StatusResponse(
            version=version,
            config=safe_config,
            whylogs_logger_status={
                k: LoggerStatusResponse(
                    dataset_timestamps=v.dataset_timestamps,
                    dataset_profiles=v.dataset_profiles,
                    segment_caches=v.segment_caches,
                    writers=v.writers,
                    pending_writables=v.pending_writables,
                    pending_views=[base64.b64encode(x).decode("utf-8") for x in v.pending_views],
                    views=[base64.b64encode(x).decode("utf-8") for x in v.views],
                )
                for k, v in status.statuses.items()
            },
        )

    @app.post("/logDebugInfo", operation_id="debug_info", dependencies=auth_dependencies, tags=["manage"])
    def log_debug_info() -> None:
        """
        Log the output of /status
        """
        s = status()
        logger.info("Server status: %s", s.model_dump_json())

    return app
