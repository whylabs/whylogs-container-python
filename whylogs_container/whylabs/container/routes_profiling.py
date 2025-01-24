# pyright: reportUnusedFunction=false
import logging
from typing import Optional

import orjson
from fastapi import Depends, FastAPI, Request

from whylogs.api.logger.experimental.logger.actor.process_rolling_logger_messages import (
    FlushMessage,
    RawLogEmbeddingsMessage,
    RawLogMessage,
    RawPubSubEmbeddingMessage,
    RawPubSubMessage,
)
from whylogs.api.logger.experimental.logger.actor.time_util import current_time_ms
from whylogs_container.whylabs.container.config import SynchronousConfig
from whylogs_container.whylabs.container.dependencies.api_key import WhyLabsApiKey, api_key_dependency
from whylogs_container.whylabs.container.requests import LogEmbeddingRequest, LogRequest
from whylogs_container.whylabs.container.server_config import get_server_config
from whylogs_container.whylabs.llm_validation.validator import ValidationActor
from whylogs_container.whylabs.logger.logger import ContainerProcessRollingLogger

logger = logging.getLogger("routes")


def init_routes_profiling(
    app: FastAPI,
    whylogs_logger: ContainerProcessRollingLogger,
    validator: Optional[ValidationActor],
    config_actor: SynchronousConfig,
) -> FastAPI:
    server_config = get_server_config()
    auth_dependencies = [Depends(api_key_dependency())] if not server_config.auth_disabled() else []

    @app.post("/log", dependencies=auth_dependencies, operation_id="log", summary="Profile tabular data", tags=["profile"])
    async def log(_raw_request: Request, _request: LogRequest, api_key: WhyLabsApiKey = Depends(api_key_dependency())) -> None:
        """
        Profile tabular data. The Swagger UI isn't able to call this currently.

        ## Sample curl request:

        ```bash
        curl -X 'POST' \
        -H "X-API-Key: <password>" \
        -H "Content-Type: application/json" \
        'http://localhost:8000/log' \
        --data-raw '{
            "datasetId": "model-62",
            "multiple": {
                "columns": [ "age", "workclass", "fnlwgt", "education" ],
                "data": [
                    [ 25, "Private", 226802, "11th" ]
                ]
            }
        }'
        ```
        ## Sample Python client request:
        ```python
        from whylogs_container_client import AuthenticatedClient
        import whylogs_container_client.api.profile.log as Log
        from whylogs_container_client.models import LogRequest, LogMultiple
        from datetime import datetime

        client = AuthenticatedClient(base_url="http://localhost:8000", token="password", prefix="", auth_header_name="X-API-Key")

        data = LogRequest(
            dataset_id="model-1",
            timestamp=int(datetime.now().timestamp() * 1000),
            multiple=LogMultiple(
                columns=["col1", "col2"],
                data=[[1, 2], [3, 4]],
            )
        )

        response = Log.sync_detailed(client=client, json_body=data)
        if response.status_code != 200:
            raise Exception(f"Failed to log data. Status code: {response.status_code}")
        # API is async, it won't fail and has no return body
        ```

        ## Sample Python request (using `requests`):
        ```python
        import requests

        # Define your API key
        api_key = "<password>"

        # API endpoint
        url = 'http://localhost:8000/log'

        # Sample data
        data = {
            "datasetId": "model-62",
            "multiple": {
                "columns": ["age", "workclass", "fnlwgt", "education"],
                "data": [
                    [25, "Private", 226802, "11th"]
                ]
            }
        }

        # Make the POST request
        headers = {"X-API-Key": api_key}
        response = requests.post(url, json=data, headers=headers)
        ```

        """
        message_body = _request.to_log_request_dict(api_key)
        b: bytes = orjson.dumps(message_body)
        whylogs_logger.send(RawLogMessage(request=b, request_time=current_time_ms()))

    @app.post(
        "/log-embeddings", dependencies=auth_dependencies, operation_id="log_embeddings", summary="Profile embeddings", tags=["profile"]
    )
    async def log_embeddings(
        _raw_request: Request, _request: LogEmbeddingRequest, api_key: WhyLabsApiKey = Depends(api_key_dependency())
    ) -> None:
        """
        This endpoint requires a custom configuration to set up before hand. See https://docs.whylabs.ai/docs/integrations-whylogs-container/
        for setting up embeddings support.

        Log embeddings data. The Swagger UI isn't able to call this currently.

        ## Sample curl request:

        ```bash
        curl -X 'POST' \
        -H "X-API-Key: <password>" \
        -H "Content-Type: application/octet-stream" \
        'http://localhost:8000/log-embeddings' \
        --data-raw '{
            "datasetId": "model-62",
            "timestamp": 1634235000,
            "embeddings": {
                "embeddings": [[0.12, 0.45, 0.33, 0.92]]
            }
        }'
        ```

        ## Sample Python request (using `requests`):
        ```python
        import requests

        # Define your API key
        api_key = "<password>"

        # API endpoint
        url = 'http://localhost:8000/log-embeddings'

        # Sample data
        data = {
            "datasetId": "model-62",
            "timestamp": 1634235000,  # an example timestamp
            "embeddings": {
                "embeddings": [[0.12, 0.45, 0.33, 0.92]]
            }
        }

        # Make the POST request
        headers = {"X-API-Key": api_key, "Content-Type": "application/octet-stream"}
        response = requests.post(url, json=data, headers=headers)
        ```
        """
        message_body = _request.to_log_embedding_request_dict(api_key)
        b: bytes = orjson.dumps(message_body)
        whylogs_logger.send(RawLogEmbeddingsMessage(request=b, request_time=current_time_ms()))

    @app.post("/log-pubsub", operation_id="log_pubsub", dependencies=auth_dependencies, tags=["profile"])
    async def log_pubsub(_raw_request: Request) -> None:
        if server_config.tenancy_mode == "MULTI":
            raise Exception("PubSub is not supported in multi-tenancy mode")

        b: bytes = await _raw_request.body()
        whylogs_logger.send(RawPubSubMessage(request=b, request_time=current_time_ms()))

    @app.post("/log-pubsub-embeddings", operation_id="log_pubsub_embedding", dependencies=auth_dependencies, tags=["profile"])
    async def log_pubsub_embeddings(_raw_request: Request) -> None:
        if server_config.tenancy_mode == "MULTI":
            raise Exception("PubSub is not supported in multi-tenancy mode")

        b: bytes = await _raw_request.body()
        whylogs_logger.send(RawPubSubEmbeddingMessage(request=b, request_time=current_time_ms()))

    @app.post("/publish", operation_id="publish", dependencies=auth_dependencies, tags=["manage"])
    def publish_profiles() -> None:
        whylogs_logger.send(FlushMessage())

    return app
