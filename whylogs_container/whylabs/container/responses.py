from enum import Enum
from typing import Any, Dict, List, Optional, cast

from fastapi import Request
from pydantic import BaseModel
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response

from langkit.core.validation import ValidationResult
from langkit.core.workflow import RunPerf
from whylogs_container.whylabs.container.config import ConfigInstance
from whylogs_container.whylabs.container.dependencies.api_key import WhyLabsApiKey
from whylogs_container.whylabs.container.requests import LLMValidateRequest
from whylogs_container.whylabs.container.server_config import get_server_config
from whylogs_container.whylabs.container.version import version
from whylogs_container.whylabs.llm_validation.actions import Action, get_action, worst_action
from whylogs_container.whylabs.llm_validation.validator import ReturnType


class LoggerStatusResponse(BaseModel):
    dataset_timestamps: int
    dataset_profiles: int
    segment_caches: int
    writers: int
    pending_writables: int
    pending_views: List[str]
    """
    Pending views are ones that have already been staged for writing. This happens
    whenever the rolling logger hits the time interal for uploading its internal state.
    If these fail to upload then they'll remain pending until the next attempt.
    """
    views: List[str]
    """
    Views are currently being added to with new log messages. There haven't been any attempts
    to write these yet.
    """


class StatusResponse(BaseModel):
    version: str
    whylogs_logger_status: Dict[str, LoggerStatusResponse]
    config: Dict[str, Any]


class MetadataField(Enum):
    POLICY_ID = "policy_id"
    POLICY_VERSION = "policy_version"
    CONTAINER_VERSION = "container_version"
    POLICY_LAST_UPDATED_MS = "policy_last_updated_ms"
    METRIC_METADATA = "metrics"


# Need a type for this because the langkit type references pandas, which won't serialize automatically
class EvaluationResult(BaseModel):
    metrics: List[Dict[str, Any]]
    validation_results: ValidationResult
    perf_info: Optional[RunPerf]
    action: Action
    score_perf_info: Optional[RunPerf] = None
    scores: List[Dict[str, Any]] = []
    metadata: Dict[str, Any] = {}

    @staticmethod
    def from_results(
        validator_output: ReturnType,
        config: ConfigInstance,
        request: LLMValidateRequest,
        api_key: WhyLabsApiKey,
        perf_info: bool = False,
        metadata_info: bool = False,
    ) -> "EvaluationResult":
        result = validator_output.result
        score_result = validator_output.score_result
        metrics: List[Dict[str, Any]] = cast(List[Dict[str, Any]], result.metrics.to_dict(orient="records"))  # pyright: ignore[reportUnknownMemberType]
        score_metrics: List[Dict[str, Any]] = cast(
            List[Dict[str, Any]],
            score_result.metrics.to_dict(orient="records") if score_result else [],  # pyright: ignore[reportUnknownMemberType]
        )
        score_validation_results = score_result.validation_results.report if score_result else []

        action = get_action(result, config=config, dataset_id=request.dataset_id, org_id=api_key.org_id)
        score_action = (
            get_action(score_result, config=config, dataset_id=request.dataset_id, org_id=api_key.org_id) if score_result else None
        )

        policy = config.config.get_policy(request.dataset_id, org_id=api_key.org_id)

        if policy:
            metadata = {
                MetadataField.POLICY_ID.value: policy.id,
                MetadataField.POLICY_VERSION.value: policy.policy_version,
                MetadataField.CONTAINER_VERSION.value: version,
                MetadataField.POLICY_LAST_UPDATED_MS.value: config.config.last_update_time_ms,
                MetadataField.METRIC_METADATA.value: validator_output.wf_metadata,
            }
        else:
            metadata = {
                MetadataField.CONTAINER_VERSION.value: version,
                MetadataField.POLICY_LAST_UPDATED_MS.value: config.config.last_update_time_ms,
            }

        return EvaluationResult(
            metrics=metrics,
            scores=score_metrics,
            validation_results=ValidationResult(report=result.validation_results.report + score_validation_results),
            perf_info=result.perf_info if perf_info else None,
            score_perf_info=score_result.perf_info if perf_info and score_result else None,
            action=worst_action(action, score_action),
            metadata=metadata if metadata_info else {},
        )


class AvailableMetrics(BaseModel):
    metrics_names: List[str]


class WhyLabsSecureHeaders(str, Enum):
    CLIENT_VERSION_CONSTRAINT = "x-wls-verconstr"  #  Specify the client (OpenLLMTelemetry version constraint)
    SERVERSIDE_TRACED = "x-wls-sst"  #  Specify if the server is tracing
    VERSION = "x-wls-version"  # Specify the server version


def get_container_headers() -> Dict[str, str]:
    """
    Get the headers that the container will send to the client
    """
    settings = get_server_config()
    if not settings.llm_container:
        return {}

    return {
        WhyLabsSecureHeaders.CLIENT_VERSION_CONSTRAINT.value: ">=0.0.1",
        WhyLabsSecureHeaders.VERSION.value: version,
        WhyLabsSecureHeaders.SERVERSIDE_TRACED.value: "1" if not settings.disable_tracing else "0",
    }


class AddResponseHeaderMiddleWare(BaseHTTPMiddleware):
    """
    Add headers to all responses for LLM container
    """

    _config = get_server_config()

    async def dispatch(self, request: Request, call_next):
        response: Response = await call_next(request)

        headers = get_container_headers()
        for k, v in headers.items():
            response.headers[k] = v

        return response
