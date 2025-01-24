import json
import logging
import os
from enum import Enum
from functools import cache
from typing import Any, Dict, List, Optional, Tuple, cast

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http import Compression
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.propagate import extract, inject
from opentelemetry.sdk.environment_variables import OTEL_ATTRIBUTE_COUNT_LIMIT
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import SpanLimits, TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter, SimpleSpanProcessor

from langkit.core.workflow import WorkflowResult
from whylogs_container.whylabs.container.config import ConfigInstance
from whylogs_container.whylabs.container.dependencies.api_key import WhyLabsApiKey
from whylogs_container.whylabs.container.server_config import get_server_config
from whylogs_container.whylabs.container.version import version
from whylogs_container.whylabs.llm_validation.actions import get_action

logger = logging.getLogger(__name__)

_service_name = "guardrails_service"
_main_span_name = "guardrails"
_secure_prefix = "whylabs.secure"


class AttributePrefix(Enum):
    Metric = f"{_secure_prefix}.metrics"
    WorkflowAction = f"{_secure_prefix}.action"
    MetricLatency = f"{_secure_prefix}.latency"
    Score = f"{_secure_prefix}.score"
    Policy = f"{_secure_prefix}.policy"
    Metadata = f"{_secure_prefix}.metadata"
    Container = f"{_secure_prefix}.container"
    AdditionalData = f"{_secure_prefix}.additional_data"
    ValidationEvent = f"{_secure_prefix}.validation"

    DatasetId = "whylabs.dataset_id"  # 'deprecated. Use resource_id"
    ResourceId = "whylabs.resource_id"  # 'replacing dataset_id'
    ApiKeyId = "whylabs.api_key-id"
    # Not in spans, but in resource attributes
    Version = "guardrails_container.version"
    Tags = f"{_secure_prefix}.tags"


# trace_provider: Optional[TracerProvider] = None


def _create_trace_headers(child_org: Optional[str]) -> Dict[str, str]:
    server_config = get_server_config()

    if server_config.tenancy_mode == "MULTI":
        if not child_org:
            raise ValueError("Child organization id is required in multi-tenancy mode.")
        return {"X-API-Key": server_config.whylabs_api_key, "X-WhyLabs-Organization": child_org}
    else:
        return {"X-API-Key": server_config.whylabs_api_key}


def _create_trace_provider(child_org: Optional[str]) -> TracerProvider:
    server_config = get_server_config()

    resource = Resource(
        attributes={
            "service.name": _service_name,
            AttributePrefix.Version.value: version,
        }
    )

    if os.environ.get(OTEL_ATTRIBUTE_COUNT_LIMIT) is None:
        # Opt out of the hard coded limits if this env var is set as an escape hatch. I don't want to make it impossible
        # to override these if needed.
        a_lot = 10_000
        limits = SpanLimits(
            max_events=a_lot,
            max_attributes=a_lot,
            max_links=a_lot,
            max_span_attributes=a_lot,
            max_event_attributes=a_lot,
            max_link_attributes=a_lot,
            max_attribute_length=a_lot,
            max_span_attribute_length=a_lot,
        )
    else:
        limits = None

    trace_provider = TracerProvider(resource=resource, span_limits=limits)

    headers = _create_trace_headers(child_org)

    if server_config.log_level == logging.DEBUG:
        processor = SimpleSpanProcessor(
            OTLPSpanExporter(
                endpoint=server_config.trace_endpoint,
                compression=Compression.Gzip,
                headers=headers,
            )
        )
    else:
        processor = BatchSpanProcessor(
            OTLPSpanExporter(
                endpoint=server_config.trace_endpoint,
                compression=Compression.Gzip,
                headers=headers,
            )
        )
    trace_provider.add_span_processor(processor)

    if server_config.log_level == logging.DEBUG:
        trace_provider.add_span_processor(SimpleSpanProcessor(ConsoleSpanExporter()))

    return trace_provider


@cache
def get_tracer(child_org: Optional[str]) -> trace.Tracer:
    trace_provider = _create_trace_provider(child_org)
    return trace_provider.get_tracer(_service_name)


def get_current_carrier() -> Dict[str, Any]:
    carrier: Dict[str, Any] = {}
    inject(carrier)
    return carrier


def span_value(it: Any) -> Any:
    if isinstance(it, list) or isinstance(it, dict):
        return json.dumps(it)
    return it


def extract_score_causes(score_result: WorkflowResult) -> List[str]:
    """
    This function will take the output of a score workflow and return the metrics that caused the high scores
    for each ruleset that was run. The output of the score workflow will be scores for metrics like this dataframe:

        prompt.score.misuse                                                     100
        prompt.score.misuse.prompt.topics.financial                             100
        prompt.score.misuse.prompt.topics.medical                                12
        response.score.customer_experience                                       70
        response.score.customer_experience.response.sentiment.sentiment_score  None
        response.score.customer_experience.response.toxicity.toxicity_score      21
        response.score.customer_experience.response.regex.refusal                70

    Each score will be accompanied by the raw metric values as well. For example:

        prompt.score.misuse                                                     100
        prompt.score.misuse.prompt.topics.financial                             100
        prompt.score.misuse.prompt.topics.medical                                12

    Here, the `prompt.score.misuse` score was high because the `prompt.topics.financial` metric was high. The `prompt.topics.medical`
    metric was low, so it did not determine the high score. They're just maxed at the moment.

    For the sample input above, if these rulesets were configured to fail then this function will return

        ['prompt.score.misuse.prompt.topics.financial', 'response.score.customer_experience.response.regex.refusal']

    because those were the two causes of the high scores in the rulesets that were run. If there were no validation failures in
    the workflow then the output will be an empty list. It doesn't return the highest scoring metric, it returns the highest scoring
    metric that caused the score to be high enough to fail validation.
    """

    score_metrics = cast(List[Dict[str, Any]], score_result.metrics.to_dict(orient="records")) if score_result else []  # pyright: ignore[reportUnknownMemberType]
    ruleset_failures = {report.metric: report.value for report in score_result.validation_results.report}

    max_ruleset_failures: Dict[str, Tuple[str, int]] = {}

    for score_metric in score_metrics:
        for metric_name, metric_value in score_metric.items():
            for ruleset_name, ruleset_value in ruleset_failures.items():
                if ruleset_name in metric_name:
                    stripped_metric_name = metric_name
                    # stripped_metric_name = metric_name.replace(f"{ruleset_name}.", "")

                    if ruleset_value is None or ruleset_name == metric_name:
                        continue

                    if metric_value == ruleset_value:
                        max_ruleset_failures[ruleset_name] = (stripped_metric_name, metric_value)

    # for each ruleset in max_ruleset_failures, get the metric name with the highest value
    return list(set([v[0] for _k, v in max_ruleset_failures.items()]))


def trace_langkit_result(
    result: WorkflowResult,
    score_result: Optional[WorkflowResult],
    config: ConfigInstance,
    dataset_id: str,
    start_time_ns: int,
    end_time_ns: int,
    carrier: Dict[str, Any],
    api_key: WhyLabsApiKey,
    metric_metadata: Optional[Dict[str, Any]] = None,
    additional_data: Optional[Dict[str, Any]] = None,
    request_metadata: Optional[Dict[str, Any]] = None,
) -> None:
    server_config = get_server_config()
    if server_config.disable_tracing:
        logger.warning("Tracing is disabled by environment variable (DISABLE_TRACING). No traces sent.")
        return

    tracer = get_tracer(None if server_config.tenancy_mode == "SINGLE" else api_key.org_id)

    action = get_action(result, config=config, dataset_id=dataset_id, org_id=api_key.org_id)
    score_action = get_action(score_result, config=config, dataset_id=dataset_id, org_id=api_key.org_id) if score_result else None
    metrics = cast(List[Dict[str, Any]], result.metrics.to_dict(orient="records"))  # pyright: ignore[reportUnknownMemberType]
    scores = cast(List[Dict[str, Any]], score_result.metrics.to_dict(orient="records")) if score_result else []  # pyright: ignore[reportUnknownMemberType]
    policy = config.config.get_policy(dataset_id, api_key.org_id)

    ctx = extract(carrier)

    # we end the span explicitly to set the end time stamp
    span = tracer.start_span(_main_span_name, start_time=start_time_ns, context=ctx, kind=trace.SpanKind.SERVER)
    # dataset_id = None
    try:
        span.set_attribute(AttributePrefix.ApiKeyId.value, api_key.id)
        span.set_attribute(AttributePrefix.DatasetId.value, dataset_id)
        span.set_attribute(AttributePrefix.ResourceId.value, dataset_id)

        if policy:
            span.set_attribute(f"{AttributePrefix.Policy.value}.version", policy.policy_version)
            span.set_attribute(f"{AttributePrefix.Policy.value}.schema_version", policy.schema_version)
            span.set_attribute(f"{AttributePrefix.Policy.value}.id", policy.id)

        for metric_result in metrics:
            for k, v in metric_result.items():
                if "redacted" in k:
                    continue
                if v is not None:
                    span.set_attribute(f"{AttributePrefix.Metric.value}.{k}", span_value(v))

        for score_metric in scores:
            for k, v in score_metric.items():
                if v is not None:
                    span.set_attribute(f"{AttributePrefix.Score.value}.{k}", v)
        if score_result:
            if len(score_result.validation_results.report):
                failed_rulesets = set([it.metric for it in score_result.validation_results.report])
                tags = list(failed_rulesets)  # + extract_score_causes(score_result)
                span.set_attribute(AttributePrefix.Tags.value, tags)
        else:
            # If we're not using rulesets then just use the exact metric names as the tags
            tags = set([it.metric for it in result.validation_results.report])
            span.set_attribute(AttributePrefix.Tags.value, list(tags))

        for failure in result.validation_results.report + (score_result.validation_results.report if score_result else []):
            if "redacted" in failure.metric:
                continue
            event_attributes: Dict[str, Any] = {}
            for k, v in failure.__dict__.items():
                if v is not None:
                    event_attributes[k] = v
            span.add_event(AttributePrefix.ValidationEvent.value, event_attributes)

        if metric_metadata:
            for k, v in metric_metadata.items():
                if v is not None:
                    span.set_attribute(f"{AttributePrefix.Metadata.value}.metrics.{k}", span_value(v))

        if request_metadata:
            for k, v in request_metadata.items():
                if v is not None:
                    span.set_attribute(f"{AttributePrefix.Metadata.value}.request.{k}", span_value(v))

        span.set_attribute(f"{AttributePrefix.Container.value}.version", version)

        if additional_data:
            for k, v in additional_data.items():
                span.set_attribute(f"{AttributePrefix.AdditionalData.value}.{k}", span_value(v))

        # workflow latency info
        span.set_attribute(f"{AttributePrefix.MetricLatency.value}.init.total", result.perf_info.init_total_sec)
        for context_name, latency in result.perf_info.context_time_sec.items():
            span.set_attribute(f"{AttributePrefix.MetricLatency.value}.context.{context_name}", latency)
        span.set_attribute(f"{AttributePrefix.MetricLatency.value}.context.total", result.perf_info.context_total_sec)
        for context_name, latency in result.perf_info.metrics_time_sec.items():
            span.set_attribute(f"{AttributePrefix.MetricLatency.value}.metric.{context_name}", latency)
        span.set_attribute(f"{AttributePrefix.MetricLatency.value}.metric.total", result.perf_info.metrics_total_sec)
        span.set_attribute(f"{AttributePrefix.MetricLatency.value}.validation.total", result.perf_info.validation_total_sec)
        span.set_attribute(f"{AttributePrefix.MetricLatency.value}.workflow.total", result.perf_info.workflow_total_sec)
        span.set_attribute(
            f"{AttributePrefix.WorkflowAction.value}.type", str(score_action.action_type if score_action else action.action_type)
        )
        span.set_status(trace.Status(trace.StatusCode.OK))
    except Exception as e:
        logger.error(f"Error setting span attributes: {e}")
        span.set_status(trace.Status(trace.StatusCode.ERROR, str(e)))
    finally:
        span.end(end_time_ns)
