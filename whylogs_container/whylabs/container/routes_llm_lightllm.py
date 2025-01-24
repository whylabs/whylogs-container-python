# pyright: reportUnusedFunction=false
import logging
from typing import Optional

from fastapi import Depends, FastAPI, Request
from fastapi.exceptions import HTTPException

# TODO get this out of the routes file
from whylogs_container.util.coroutine_util import bool_not
from whylogs_container.whylabs.container.config import ConfigInstance
from whylogs_container.whylabs.container.dependencies.api_key import WhyLabsApiKey, api_key_dependency
from whylogs_container.whylabs.container.requests import LLMValidateRequest
from whylogs_container.whylabs.container.responses import (
    EvaluationResult,
)
from whylogs_container.whylabs.container.routes_llm import build_carrier
from whylogs_container.whylabs.container.server_config import get_server_config
from whylogs_container.whylabs.llm_validation.validator import ValidationActor
from whylogs_container.whylabs.logger.logger import ContainerProcessRollingLogger

logger = logging.getLogger("routes")


def init_lightllm_routes(
    app: FastAPI,
    whylogs_logger: Optional[ContainerProcessRollingLogger],
    validator: Optional[ValidationActor],
    config_instance: ConfigInstance,
) -> FastAPI:
    server_config = get_server_config()
    llm_dependencies = [Depends(server_config.assert_llm_endpoint)]

    @app.post(
        "/v0/integrations/lightllm/evaluate",
        dependencies=llm_dependencies,
        response_model=EvaluationResult,
        tags=["llm", "lightllm", "integrations"],
        summary="Evaluate and log a single prompt/response pair from lightllm.",
    )
    def lightllm_evaluate(
        request: LLMValidateRequest,
        _request: Request,
        log: bool = True,
        perf_info: bool = False,
        trace: bool = True,
        metadata_info: bool = False,
        api_key: WhyLabsApiKey = Depends(api_key_dependency()),
    ) -> EvaluationResult:
        """
        Run langkit evaluation and return the validation results, as well as the generated metrics.

        Args:
            log (bool, optional): Determines if logging to WhyLabs is enabled for the request. Defaults to True.
        """
        if validator is None:
            raise HTTPException(status_code=500, detail="LLM evaluation is not enabled")

        # Attach the context from the HTTP request header to this context
        carrier = build_carrier(_request)

        try:
            results = validator.evaluate(
                request, bool_not(_request.is_disconnected()), log=log, trace=trace, carrier=carrier, api_key=api_key
            )
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error validating prompt: {e}") from e

        return EvaluationResult.from_results(
            results, config=config_instance, request=request, perf_info=perf_info, metadata_info=metadata_info, api_key=api_key
        )

    return app
