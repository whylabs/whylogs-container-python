from dataclasses import dataclass, field
from enum import Enum
from functools import cache, partial
from typing import Any, Callable, Dict, List, Literal, Mapping, Optional, ParamSpec, Tuple, Union

from langkit.callbacks.library import lib as callback_lib
from langkit.core.metric import Metric, MetricCreator, WorkflowMetricConfigBuilder
from langkit.core.validation import Validator
from langkit.core.workflow import Callback
from langkit.metrics.library import lib
from langkit.transformer import SentenceTransformerTarget
from whylogs_container.whylabs.container.hashable import HashableDict
from whylogs_container.whylabs.container.metrics.library import lib as custom_lib

CallbackCreator = Callable[[Mapping[str, Any]], Callback]

callback_types: Mapping[str, CallbackCreator] = {
    "webhook.static_bearer_auth_validation_failure": lambda kwargs: callback_lib.webhook.static_bearer_auth_validation_failure(**kwargs),
    "webhook.basic_validation_failure": lambda kwargs: callback_lib.webhook.basic_validation_failure(**kwargs),
    "webhook.slack_validation_failure": lambda kwargs: callback_lib.webhook.slack_validation_failure(**kwargs),
}


@dataclass(frozen=True)
class WorkflowOptions:
    metrics: List[MetricCreator] = field(default_factory=list)
    callbacks: List[Callback] = field(default_factory=list)
    validators: List[Validator] = field(default_factory=list)


class CachedMetricCreator:
    def __init__(self, metric_creator: Callable[..., MetricCreator]):
        self.metric_creator = metric_creator

    @cache
    def __call__(self, kwargs: Optional[Mapping[str, Any]] = None) -> List[Metric]:
        if kwargs is None:
            kwargs = {}

        # A bunch of things we know have to be handled in a special way
        # TODO this can go away once the metrics switch over to embedding choice model
        sanitized: Mapping[str, Any] = {**kwargs}
        if "embedding" in kwargs and isinstance(kwargs["embedding"], Mapping):
            sanitized["embedding"] = SentenceTransformerTarget(**kwargs["embedding"])  # pyright: ignore[reportUnknownArgumentType]

        return WorkflowMetricConfigBuilder().add(self.metric_creator(**sanitized)).build().metrics


P = ParamSpec("P")

RulesetCreator = Callable[P, WorkflowOptions]


def ruleset_cache_wrapper(fn: Callable[[], RulesetCreator[P]]) -> RulesetCreator[P]:
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> WorkflowOptions:
        ruleset_fn = fn()
        options = ruleset_fn(*args, **kwargs)
        return WorkflowOptions(
            metrics=[CachedMetricCreator(lambda: options.metrics)] if options.metrics else [],
            validators=options.validators,
            callbacks=options.callbacks,
        )

    return cache(wrapper)  # pyright: ignore[reportReturnType]


ValidatorCreator = Union[Callable[[Mapping[str, Any]], Validator], Callable[[Mapping[str, Any]], List[Validator]]]


def _get_encoder_specific_metrics() -> Mapping[str, CachedMetricCreator]:
    from whylabs_llm_toolkit.data.scripts.targets import SentenceTransformerEncoder
    from whylabs_llm_toolkit.settings import get_settings

    settings = get_settings()
    encoder = SentenceTransformerEncoder.from_string(settings.DEFAULT_ENCODER)

    if encoder == SentenceTransformerEncoder.AllMiniLML6V2:
        pca_version = 8
        setfit_version = 8
        toxicity_metric = CachedMetricCreator(partial(lib.prompt.similarity.toxic, stage="prod", version=2, pca_version=pca_version))
        return {
            "prompt.pca.coordinates": CachedMetricCreator(partial(lib.prompt.pca.coordinates, stage="prod", version=pca_version)),
            "prompt.topics": CachedMetricCreator(partial(lib.prompt.topics, topics=["medicine"], stage="prod", version=setfit_version)),
            "response.pca.coordinates": CachedMetricCreator(partial(lib.response.pca.coordinates, stage="prod", version=pca_version)),
            "response.topics": CachedMetricCreator(partial(lib.response.topics, topics=["medicine"], stage="prod", version=setfit_version)),
            "prompt.similarity.injection": CachedMetricCreator(
                partial(
                    lib.prompt.similarity.injection,
                    tag="main",
                    stage="prod",
                    version=8,
                    pca_stage="prod",
                    pca_version=pca_version,
                    twoclass=False,
                )
            ),
            "prompt.similarity.financial": CachedMetricCreator(
                partial(lib.prompt.similarity.financial, stage="prod", version=2, pca_version=pca_version, twoclass=False)
            ),
            "prompt.similarity.code": CachedMetricCreator(
                partial(lib.prompt.similarity.code, stage="prod", version=2, pca_version=pca_version, twoclass=False)
            ),
            "prompt.similarity.hate": CachedMetricCreator(
                partial(lib.prompt.similarity.hate, stage="prod", version=2, pca_version=pca_version, twoclass=False)
            ),
            "prompt.similarity.medical": CachedMetricCreator(
                partial(lib.prompt.similarity.medical, stage="prod", version=2, pca_version=pca_version, twoclass=False)
            ),
            "prompt.similarity.innocuous": CachedMetricCreator(
                partial(lib.prompt.similarity.innocuous, stage="prod", version=2, pca_version=pca_version, twoclass=False)
            ),
            "prompt.similarity.toxic": toxicity_metric,
            "prompt.toxicity": toxicity_metric,
            "prompt.toxicity.toxicity_score": toxicity_metric,
            "response.toxicity": toxicity_metric,
            "response.toxicity.toxicity_score": toxicity_metric,
        }
    elif encoder == SentenceTransformerEncoder.ParaphraseMultilingualMiniLML12V2:
        pca_version = 8
        setfit_version = 8
        toxicity_metric = CachedMetricCreator(partial(lib.prompt.similarity.toxic, stage="prod", version=2, pca_version=pca_version))
        return {
            "prompt.pca.coordinates": CachedMetricCreator(partial(lib.prompt.pca.coordinates, stage="prod", version=pca_version)),
            "prompt.topics": CachedMetricCreator(partial(lib.prompt.topics, topics=["medicine"], stage="prod", version=setfit_version)),
            "response.pca.coordinates": CachedMetricCreator(partial(lib.response.pca.coordinates, stage="prod", version=pca_version)),
            "response.topics": CachedMetricCreator(partial(lib.response.topics, topics=["medicine"], stage="prod", version=setfit_version)),
            "prompt.similarity.injection": CachedMetricCreator(
                partial(
                    lib.prompt.similarity.injection,
                    tag="main",
                    stage="prod",
                    version=8,
                    pca_stage="prod",
                    pca_version=pca_version,
                    twoclass=False,
                )
            ),
            "prompt.similarity.financial": CachedMetricCreator(
                partial(lib.prompt.similarity.financial, stage="prod", version=2, pca_version=pca_version, twoclass=False)
            ),
            "prompt.similarity.code": CachedMetricCreator(
                partial(lib.prompt.similarity.code, stage="prod", version=2, pca_version=pca_version, twoclass=False)
            ),
            "prompt.similarity.hate": CachedMetricCreator(
                partial(lib.prompt.similarity.hate, stage="prod", version=2, pca_version=pca_version, twoclass=False)
            ),
            "prompt.similarity.medical": CachedMetricCreator(
                partial(lib.prompt.similarity.medical, stage="prod", version=2, pca_version=pca_version, twoclass=False)
            ),
            "prompt.similarity.innocuous": CachedMetricCreator(
                partial(lib.prompt.similarity.innocuous, stage="prod", version=2, pca_version=pca_version, twoclass=False)
            ),
            "prompt.similarity.toxic": toxicity_metric,
            "prompt.toxicity": toxicity_metric,
            "prompt.toxicity.toxicity_score": toxicity_metric,
            "response.toxicity": toxicity_metric,
            "response.toxicity.toxicity_score": toxicity_metric,
        }
    elif encoder == SentenceTransformerEncoder.BGESmallEn_V1_5:
        pca_version = 6
        setfit_version = 6
        toxicity_metric = CachedMetricCreator(partial(lib.prompt.similarity.toxic, stage="prod", version=2, pca_version=pca_version))
        return {
            "prompt.pca.coordinates": CachedMetricCreator(partial(lib.prompt.pca.coordinates, stage="prod", version=pca_version)),
            "prompt.topics": CachedMetricCreator(partial(lib.prompt.topics, topics=["medicine"], stage="prod", version=setfit_version)),
            "response.pca.coordinates": CachedMetricCreator(partial(lib.response.pca.coordinates, stage="prod", version=pca_version)),
            "response.topics": CachedMetricCreator(partial(lib.response.topics, topics=["medicine"], stage="prod", version=setfit_version)),
            "prompt.similarity.injection": CachedMetricCreator(
                partial(
                    lib.prompt.similarity.injection,
                    tag="main",
                    stage="prod",
                    version=6,
                    pca_stage="prod",
                    pca_version=pca_version,
                    twoclass=False,
                )
            ),
            "prompt.similarity.financial": CachedMetricCreator(
                partial(lib.prompt.similarity.financial, stage="prod", version=2, pca_version=pca_version, twoclass=False)
            ),
            "prompt.similarity.code": CachedMetricCreator(
                partial(lib.prompt.similarity.code, stage="prod", version=2, pca_version=pca_version, twoclass=False)
            ),
            "prompt.similarity.hate": CachedMetricCreator(
                partial(lib.prompt.similarity.hate, stage="prod", version=2, pca_version=pca_version, twoclass=False)
            ),
            "prompt.similarity.medical": CachedMetricCreator(
                partial(lib.prompt.similarity.medical, stage="prod", version=2, pca_version=pca_version, twoclass=False)
            ),
            "prompt.similarity.innocuous": CachedMetricCreator(
                partial(lib.prompt.similarity.innocuous, stage="prod", version=2, pca_version=pca_version, twoclass=False)
            ),
            "prompt.similarity.toxic": toxicity_metric,
            "prompt.toxicity": toxicity_metric,
            "prompt.toxicity.toxicity_score": toxicity_metric,
            "response.toxicity": toxicity_metric,
            "response.toxicity.toxicity_score": toxicity_metric,
        }
    else:
        raise ValueError(f"Unknown encoder {encoder}")


@cache
def get_metric_names() -> Mapping[str, CachedMetricCreator]:
    encoder_specific_metrics = _get_encoder_specific_metrics()

    # These can't be included in the preset.all preset because they explicitly need args
    metrics_with_required_args: Mapping[str, CachedMetricCreator] = {
        "CUSTOM_COLUMN.similarity.CUSTOM_COLUMN_2": CachedMetricCreator(lib.CUSTOM_COLUMN.similarity.CUSTOM_COLUMN_2),
        "prompt.similarity.CUSTOM_COLUMN": CachedMetricCreator(lib.prompt.similarity.CUSTOM_COLUMN),
        "response.similarity.CUSTOM_COLUMN": CachedMetricCreator(lib.response.similarity.CUSTOM_COLUMN),
    }

    metrics: Mapping[str, CachedMetricCreator] = {
        "prompt.stats": CachedMetricCreator(lib.prompt.stats),
        "prompt.stats.token_count": CachedMetricCreator(lib.prompt.stats.token_count),
        "prompt.text_stat": CachedMetricCreator(lib.prompt.stats),
        "prompt.stats.char_count": CachedMetricCreator(lib.prompt.stats.char_count),
        "prompt.stats.flesch_reading_ease": CachedMetricCreator(lib.prompt.stats.flesch_reading_ease),
        "prompt.stats.sentence_count": CachedMetricCreator(lib.prompt.stats.sentence_count),
        "prompt.stats.grade": CachedMetricCreator(lib.prompt.stats.grade),
        "prompt.stats.lexicon_count": CachedMetricCreator(lib.prompt.stats.lexicon_count),
        "prompt.stats.letter_count": CachedMetricCreator(lib.prompt.stats.letter_count),
        "prompt.stats.difficult_words": CachedMetricCreator(lib.prompt.stats.difficult_words),
        "prompt.regex": CachedMetricCreator(lib.prompt.regex),
        "prompt.regex.ssn": CachedMetricCreator(lib.prompt.regex.ssn),
        "prompt.regex.phone_number": CachedMetricCreator(lib.prompt.regex.phone_number),
        "prompt.regex.email_address": CachedMetricCreator(lib.prompt.regex.email_address),
        "prompt.regex.mailing_address": CachedMetricCreator(lib.prompt.regex.mailing_address),
        "prompt.regex.credit_card_number": CachedMetricCreator(lib.prompt.regex.credit_card_number),
        "prompt.regex.url": CachedMetricCreator(lib.prompt.regex.url),
        "prompt.pii": CachedMetricCreator(lib.prompt.pii),
        "prompt.similarity.context": CachedMetricCreator(lib.prompt.similarity.context),
        "prompt.sentiment": CachedMetricCreator(lib.prompt.sentiment),
        "prompt.sentiment.sentiment_score": CachedMetricCreator(lib.prompt.sentiment.sentiment_score),
        "prompt.util.embeddings": CachedMetricCreator(lib.prompt.util.embedding),
        "response.hallucination.hallucination_score": CachedMetricCreator(lib.response.hallucination.hallucination_score),
        "response.stats": CachedMetricCreator(lib.response.stats),
        "response.stats.token_count": CachedMetricCreator(lib.response.stats.token_count),
        "response.text_stat": CachedMetricCreator(lib.response.stats),
        "response.stats.char_count": CachedMetricCreator(lib.response.stats.char_count),
        "response.stats.flesch_reading_ease": CachedMetricCreator(lib.response.stats.flesch_reading_ease),
        "response.stats.sentence_count": CachedMetricCreator(lib.response.stats.sentence_count),
        "response.stats.grade": CachedMetricCreator(lib.response.stats.grade),
        "response.stats.lexicon_count": CachedMetricCreator(lib.response.stats.lexicon_count),
        "response.stats.letter_count": CachedMetricCreator(lib.response.stats.letter_count),
        "response.stats.difficult_words": CachedMetricCreator(lib.response.stats.difficult_words),
        "response.regex": CachedMetricCreator(lib.response.regex),
        "response.regex.refusal": CachedMetricCreator(lib.response.regex.refusal),
        "response.regex.ssn": CachedMetricCreator(lib.response.regex.ssn),
        "response.regex.phone_number": CachedMetricCreator(lib.response.regex.phone_number),
        "response.regex.email_address": CachedMetricCreator(lib.response.regex.email_address),
        "response.regex.mailing_address": CachedMetricCreator(lib.response.regex.mailing_address),
        "response.regex.credit_card_number": CachedMetricCreator(lib.response.regex.credit_card_number),
        "response.regex.url": CachedMetricCreator(lib.response.regex.url),
        "response.pii": CachedMetricCreator(lib.response.pii),
        "response.similarity": CachedMetricCreator(lib.response.similarity),
        "response.similarity.refusal": CachedMetricCreator(custom_lib.similarity.custom_refusal),
        "response.similarity.prompt": CachedMetricCreator(lib.response.similarity.prompt),
        "response.similarity.context": CachedMetricCreator(lib.response.similarity.context),
        "response.sentiment": CachedMetricCreator(lib.response.sentiment),
        "response.sentiment.sentiment_score": CachedMetricCreator(lib.response.sentiment.sentiment_score),
        "response.util.embeddings": CachedMetricCreator(lib.response.util.embedding),
        **encoder_specific_metrics,
    }

    # We have to recreate all of the presets here rather than use the ones we made in toolkit because we need them to
    # reference these cached metrics to avoid the overhead of re-creating them whenever new workflows or configuration
    # updates happen

    # This one has to be first otherwise it will include the other presets as well

    presets: Mapping[str, CachedMetricCreator] = {}

    presets["presets.all"] = CachedMetricCreator(lambda: list(metrics.values()))

    presets["prompt.similarity"] = CachedMetricCreator(
        lambda: [metrics["prompt.similarity.injection"], metrics["prompt.similarity.context"]]
    )

    def recommended(**kwargs: Dict[str, Any]) -> MetricCreator:
        prompt = kwargs.get("prompt", True)
        response = kwargs.get("response", True)

        prompt_metrics = [
            metrics["prompt.pii"],
            metrics["prompt.stats.token_count"],
            metrics["prompt.stats.char_count"],
            metrics["prompt.similarity.injection"],
        ]

        response_metrics = [
            metrics["response.pii"],
            metrics["response.stats.token_count"],
            metrics["response.stats.char_count"],
            metrics["response.stats.flesch_reading_ease"],
            metrics["response.sentiment.sentiment_score"],
            metrics["response.toxicity.toxicity_score"],
            metrics["response.similarity.refusal"],
        ]

        return [
            *(prompt_metrics if prompt else []),
            *(response_metrics if response else []),
        ]

    presets["presets.recommended"] = CachedMetricCreator(recommended)

    return {**metrics, **presets, **metrics_with_required_args}


Behavior = Literal["observe", "flag", "block"]

Sensitivity = Literal["low", "medium", "high"]


class RulesetLibrary:
    @staticmethod
    def prompt_misuse(behavior: Behavior, sensitivity: Sensitivity, topics: Tuple[str, ...]) -> WorkflowOptions:
        cached_metrics = get_metric_names()
        if not topics:
            return WorkflowOptions(metrics=[])

        metrics: List[MetricCreator] = []
        topic_metric = cached_metrics["prompt.topics"]
        metrics.append(lambda: topic_metric(HashableDict({"topics": tuple(topics)})))

        return WorkflowOptions(metrics=metrics)

    @staticmethod
    def response_misuse(behavior: Behavior, sensitivity: Sensitivity, topics: Tuple[str, ...]) -> WorkflowOptions:
        pii_metric = get_metric_names()["response.pii"]
        return WorkflowOptions(metrics=[pii_metric])

    @staticmethod
    def prompt_bad_actors(behavior: Behavior, sensitivity: Sensitivity) -> WorkflowOptions:
        cached_metrics = get_metric_names()
        return WorkflowOptions(
            metrics=[
                cached_metrics["prompt.similarity.injection"],
            ]
        )

    @staticmethod
    def response_bad_actors(behavior: Behavior, sensitivity: Sensitivity) -> WorkflowOptions:
        return WorkflowOptions(metrics=[])

    @staticmethod
    def prompt_customer_experience(behavior: Behavior, sensitivity: Sensitivity) -> WorkflowOptions:
        cached_metrics = get_metric_names()
        return WorkflowOptions(
            metrics=[
                cached_metrics["prompt.sentiment.sentiment_score"],
                cached_metrics["prompt.pii"],
            ]
        )

    @staticmethod
    def response_customer_experience(behavior: Behavior, sensitivity: Sensitivity) -> WorkflowOptions:
        cached_metrics = get_metric_names()
        return WorkflowOptions(
            metrics=[
                cached_metrics["response.sentiment.sentiment_score"],
                cached_metrics["response.toxicity.toxicity_score"],
                cached_metrics["response.regex.refusal"],
            ]
        )

    @staticmethod
    def prompt_truthfulness(
        behavior: Behavior, sensitivity: Sensitivity, rag_enabled: bool, hallucinations_enabled: bool
    ) -> WorkflowOptions:
        return WorkflowOptions(metrics=[])

    @staticmethod
    def response_truthfulness(
        behavior: Behavior, sensitivity: Sensitivity, rag_enabled: bool, hallucinations_enabled: bool
    ) -> WorkflowOptions:
        metric_creators: List[MetricCreator] = [lib.response.similarity.prompt()]
        cached_metrics = get_metric_names()

        if rag_enabled:
            metric_creators.append(cached_metrics["response.similarity.context"])
            metric_creators.append(cached_metrics["prompt.similarity.context"])

        if hallucinations_enabled:
            cached_metrics["response.hallucination.hallucination_score"]

        return WorkflowOptions(metrics=metric_creators)

    @staticmethod
    def prompt_cost(behavior: Behavior, sensitivity: Sensitivity) -> WorkflowOptions:
        cached_metrics = get_metric_names()
        return WorkflowOptions(
            metrics=[
                cached_metrics["prompt.stats.char_count"],
                cached_metrics["prompt.stats.token_count"],
            ]
        )

    @staticmethod
    def response_cost(behavior: Behavior, sensitivity: Sensitivity) -> WorkflowOptions:
        cached_metrics = get_metric_names()
        return WorkflowOptions(
            metrics=[
                cached_metrics["response.stats.char_count"],
                cached_metrics["response.stats.token_count"],
            ]
        )

    @staticmethod
    def prompt_util(behavior: Behavior, sensitivity: Sensitivity) -> WorkflowOptions:
        cached_metrics = get_metric_names()
        return WorkflowOptions(metrics=[cached_metrics["prompt.pca.coordinates"]])

    @staticmethod
    def response_util(behavior: Behavior, sensitivity: Sensitivity) -> WorkflowOptions:
        cached_metrics = get_metric_names()
        return WorkflowOptions(metrics=[cached_metrics["response.pca.coordinates"]])


class CachedRulesets(Enum):
    # These are wrapped in lambda to avoid touching the llm dependencies at import time from the non llm container
    prompt_cost = ruleset_cache_wrapper(lambda: RulesetLibrary.prompt_cost)
    response_cost = ruleset_cache_wrapper(lambda: RulesetLibrary.response_cost)
    prompt_util = ruleset_cache_wrapper(lambda: RulesetLibrary.prompt_util)
    response_util = ruleset_cache_wrapper(lambda: RulesetLibrary.response_util)
    prompt_customer_experience = ruleset_cache_wrapper(lambda: RulesetLibrary.prompt_customer_experience)
    response_customer_experience = ruleset_cache_wrapper(lambda: RulesetLibrary.response_customer_experience)
    prompt_misuse = ruleset_cache_wrapper(lambda: RulesetLibrary.prompt_misuse)
    response_misuse = ruleset_cache_wrapper(lambda: RulesetLibrary.response_misuse)
    prompt_bad_actors = ruleset_cache_wrapper(lambda: RulesetLibrary.prompt_bad_actors)
    response_bad_actors = ruleset_cache_wrapper(lambda: RulesetLibrary.response_bad_actors)
    prompt_truthfulness = ruleset_cache_wrapper(lambda: RulesetLibrary.prompt_truthfulness)
    response_truthfulness = ruleset_cache_wrapper(lambda: RulesetLibrary.response_truthfulness)
