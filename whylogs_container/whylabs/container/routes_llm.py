# pyright: reportUnusedFunction=false
import logging
import os
from typing import Any, Dict, Optional

from fastapi import Depends, FastAPI, Request
from fastapi.exceptions import HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from opentelemetry.propagate import extract, inject

# TODO get this out of the routes file
from whylogs_container.util.coroutine_util import bool_not
from whylogs_container.whylabs.container.aws.sqs_poller import SQSPoller
from whylogs_container.whylabs.container.config import ConfigInstance, ConfigState
from whylogs_container.whylabs.container.dependencies.api_key import WhyLabsApiKey, api_key_dependency
from whylogs_container.whylabs.container.otel import get_current_carrier
from whylogs_container.whylabs.container.policy import get_policy_org_id
from whylogs_container.whylabs.container.policy.policy_parsing import load_policy_from_str
from whylogs_container.whylabs.container.requests import DebugLLMValidateRequest, EmbeddingRequest, LLMValidateRequest
from whylogs_container.whylabs.container.responses import (
    AvailableMetrics,
    EvaluationResult,
)
from whylogs_container.whylabs.container.server_config import get_server_config
from whylogs_container.whylabs.llm_validation.validator import AsyncValidationMessage, ReturnType, ValidationActor
from whylogs_container.whylabs.logger.logger import ContainerProcessRollingLogger

logger = logging.getLogger("routes")


def build_carrier(request: Request) -> Dict[str, Any]:
    ctx = extract(request.headers)
    carrier: Dict[str, Any] = {}
    inject(carrier, ctx)
    return carrier


def init_llm_routes(
    app: FastAPI,
    whylogs_logger: Optional[ContainerProcessRollingLogger],
    validator: Optional[ValidationActor],
    config_instance: ConfigInstance,
    sqs_poller: Optional[SQSPoller],
) -> FastAPI:
    server_config = get_server_config()
    llm_dependencies = [Depends(server_config.assert_llm_endpoint)]

    @app.post(
        "/log/llm",
        dependencies=llm_dependencies,
        operation_id="log_llm",
        summary="Evaluate and log a single prompt/response pair using langkit asynchronously.",  # TODO this looks like the title
        tags=["llm"],
    )
    def log_llm(request: LLMValidateRequest, api_key: WhyLabsApiKey = Depends(api_key_dependency())) -> None:
        """
        This is a convenience wrapper around the llm request type for calling /log, which accepts bulk data.
        """
        if validator is None:
            raise HTTPException(status_code=500, detail="LLM evaluation is not enabled")

        msg = AsyncValidationMessage(
            request=request.model_dump_json(by_alias=True).encode("utf-8"),
            timestamp=request.timestamp,
            carrier=get_current_carrier(),
            api_key=api_key,
        )
        validator.send(msg)

    @app.post(
        "/evaluate/sqs",
        dependencies=llm_dependencies,
        tags=["llm"],
        operation_id="evaluate_sqs",
        summary="Evaluate and log a single prompt/response pair asynchronously using sqs.",
    )
    def evaluate_sqs(request: LLMValidateRequest):
        if not sqs_poller:
            raise HTTPException(status_code=500, detail="SQS is not enabled")

        sqs_poller.send_message_to_main_queue(request)

    @app.post(
        "/evaluate",
        dependencies=llm_dependencies,
        response_model=EvaluationResult,
        tags=["llm"],
        operation_id="evaluate",
        summary="Evaluate and log a single prompt/response pair using langkit.",
    )
    def evaluate(
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

    @app.post(
        "/debug/evaluate",
        dependencies=llm_dependencies,
        response_model=EvaluationResult,
        tags=["debug"],
        operation_id="debug_evaluate",
        summary="Evaluate and log a single prompt/response pair using langkit and a policy file.",
    )
    def debug_evaluate(request: DebugLLMValidateRequest, api_key: WhyLabsApiKey = Depends(api_key_dependency())) -> EvaluationResult:
        settings = get_server_config()
        if not settings.debug_endpoint_enabled:
            raise HTTPException(
                status_code=404,
                detail="This endpoint is not enabled. Set DEBUG_ENDPOINT_ENABLED to True in the config or environment.",
            )

        try:
            policy = load_policy_from_str(request.policy)
            config = ConfigState(local_policies=tuple([policy]))
            org_id = get_policy_org_id(policy)
            wf = config.get_langkit_workflow(request.dataset_id, org_id=org_id)
            result = wf.run(request.to_dataframe(), options=request.options)
            score_wf = config.get_langkit_score_workflow(request.dataset_id, org_id=org_id)
            score_result = score_wf.run(result.metrics) if score_wf else None
            return_type = ReturnType(result=result, score_result=score_result, wf_metadata=wf.get_metric_metadata())

            return EvaluationResult.from_results(
                return_type, config=ConfigInstance(config), request=request, perf_info=True, api_key=api_key
            )
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error evaluating input: {e}") from e

    @app.post(
        "/debug/embeddings",
        dependencies=llm_dependencies,
        response_model=EvaluationResult,
        tags=["debug"],
        operation_id="debug_embeddings",
        summary="Generate embeddings for inputs",
    )
    def debug_embeddings(
        request: EmbeddingRequest,
        _request: Request,
        api_key: WhyLabsApiKey = Depends(api_key_dependency()),
    ) -> EvaluationResult:
        validate_request = LLMValidateRequest(
            datasetId="_embeddings",
            prompt=request.prompt,
            response=request.response,
            context=request.context,
        )
        # This endpoint doesn't really want to use real policy files, it has to always select the built in _embeddings policy. The
        # results of this are never actually sente anywhere, its just for getting a hold of the embeddings to debug things.
        fake_api_key = WhyLabsApiKey.parse("xxxxxx.xxxxxxx:default")
        return evaluate(validate_request, _request, log=False, perf_info=False, trace=False, metadata_info=False, api_key=fake_api_key)

    @app.get(
        "/list_metrics",
        dependencies=llm_dependencies,
        tags=["llm"],
        operation_id="list_metrics",
        summary="Get a list of available metrics that can be referenced in policies.",
    )
    def list_metrics() -> AvailableMetrics:
        """
        Get a list of available metrics that can be referenced in policies.
        """
        from whylogs_container.whylabs.container.metrics.cached_metrics import get_metric_names

        return AvailableMetrics(metrics_names=list(get_metric_names().keys()))

    @app.get(
        "/policy",
        dependencies=llm_dependencies,
        tags=["llm"],
        operation_id="policy",
        summary="Get the JSON schema for policy files",
    )
    def get_policy(schema_version: Optional[str] = None) -> Dict[str, Any]:
        from whylogs_container.whylabs.container.policy import get_union_schema, policy_versions

        if schema_version:
            policy = policy_versions.get(schema_version)

            if policy is None:
                raise HTTPException(
                    status_code=404, detail="Policy schema version not found. Supported versions: " + ", ".join(policy_versions.keys())
                )

            return policy.model_json_schema()
        else:
            return get_union_schema()

    @app.get(
        "/debug/policies",
        dependencies=llm_dependencies,
        tags=["debug", "llm"],
        operation_id="debug_policies",
        summary="Dump debug information about the known policies.",
    )
    def debug_policies(api_key: WhyLabsApiKey = Depends(api_key_dependency())) -> Dict[str, Any]:
        # This requires an api key to mitigate anything potentially sensitive getting retrieved from the policy files
        policies = config_instance.config.get_policies()
        container_config = config_instance.config.get_container_config()

        if container_config and container_config.langkit_config:
            python_configuration = list(container_config.langkit_config.keys())
        else:
            python_configuration = []

        return {
            "python_configuration": {
                "description": "All of the datasets that have configuration defined via custom Python, as opposed to just yaml policies. "
                "If these also have yaml policies associated then they'll be merged together.",
                "values": python_configuration,
            },
            "policies": {
                "description": "All known yaml policies present on the container. The sources of these policies include local files, s3 "
                "downloads, and WhyLabs platform downloads, based on the configuration of the container.",
                "values": policies,
            },
        }

    @app.get("/health/llm/deep", operation_id="health_llm_deep", tags=["manage"])
    def deep_health(_request: Request, api_key: WhyLabsApiKey = Depends(api_key_dependency())) -> None:
        if validator is None:
            raise HTTPException(status_code=500, detail="LLM evaluation is not enabled")

        request = LLMValidateRequest(datasetId="default", prompt="test", response="test")
        results = validator.evaluate(
            request, active_predicate=bool_not(_request.is_disconnected()), log=False, trace=False, api_key=api_key
        )

        EvaluationResult.from_results(results, config=config_instance, request=request, perf_info=True, api_key=api_key)

    policy_app_dir = "policy-editor/dist/production"

    if not os.path.exists(policy_app_dir):
        message = f"Policy editor not found at {policy_app_dir} and POLICY_EDITOR_REQUIRED is set to True"
        if server_config.policy_editor_required:
            raise FileNotFoundError(message)
        else:
            logger.warning(f"Policy editor not found at {policy_app_dir}. Disabling policy editor endpoint.")
    else:
        app.mount("/assets", StaticFiles(directory=f"{policy_app_dir}/assets"), name="policy_editor_assets")

    @app.get(
        "/ui/policy",
        dependencies=[Depends(server_config.assert_llm_endpoint)],
        tags=["ui"],
        operation_id="ui_policy",
        summary="Interactive policy editor. Visit this endpoint in a browser.",
    )
    def ui_policy():
        # Just return the content of ./policy-validator-app/index.html
        # This will fail at runtime if the policy editor assets were never built
        return FileResponse(f"{policy_app_dir}/index.html")

    return app
