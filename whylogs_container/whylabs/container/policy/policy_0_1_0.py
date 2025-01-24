from functools import reduce
from typing import Any, Generic, List, Literal, Mapping, Optional, Protocol, Sequence, Tuple, TypeVar, Union, cast

from pydantic import BaseModel, ConfigDict, Field, field_validator
from typing_extensions import NotRequired, TypedDict

from langkit.callbacks.library import BasicValidationFailureOptions, SlackValidationFailureOptions, StaticBearerAuthValidationFailureOptions
from langkit.core.validation import ValidationFailureLevel, Validator
from langkit.metrics.library import (
    CoordinatesOptions,
    CustomColumnSimilarityOptions,
    EmbeddingOptions,
    HallucinationScoreOptions,
    InjectionOptions,
    PiiOptions,
    PresetOptions,
    PromptSimilarityCustomColumnOptions,
    RefusalOptions,
    ResponseSimilarityCustomColumnOptions,
    TokenCountOptions,
    TopicsOptions,
    ToxicityScoreOptions,
)
from langkit.validators.comparison import (
    ConstraintValidator,
)
from langkit.validators.library import ConstraintOptions, ConstraintValidatorOptions, MultiColumnConstraintOptions
from langkit.validators.library import lib as validation_lib
from whylogs_container.whylabs.container.hashable import HashableDict, HashableList
from whylogs_container.whylabs.container.metrics.cached_metrics import CachedRulesets, callback_types, get_metric_names
from whylogs_container.whylabs.container.strict_container_types import StrictLangkitOptions


class BaseValidator(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    # validator: Literal["constraint", "multi_column_constraint"]
    # options: Optional[Union[ConstraintValidatorOptions, MultiColumnConstraintValidatorOptions]] = None

    @field_validator("options", mode="after", check_fields=False)
    def hashable_dicts(cls, it: Any):
        if isinstance(it, HashableDict):
            return it

        if isinstance(it, list):
            return HashableList.from_list(it)  # pyright: ignore[reportUnknownArgumentType]

        if isinstance(it, dict):
            return HashableDict.from_dict(it)  # pyright: ignore[reportUnknownArgumentType]

        return it


class PolicyConstraintValidator(BaseValidator):
    model_config = ConfigDict(frozen=True, extra="forbid")
    validator: Literal["constraint"]
    options: ConstraintOptions = Field(json_schema_extra={"additionalProperties": False})


class PolicyMultiColumnConstraintValidator(BaseValidator):
    model_config = ConfigDict(frozen=True, extra="forbid")
    validator: Literal["multi_column_constraint"]
    options: MultiColumnConstraintOptions = Field(json_schema_extra={"additionalProperties": False})


Validators = Union[PolicyConstraintValidator, PolicyMultiColumnConstraintValidator]


# This is a variant of the constraint options that doesn't require specifying the target_metric
# This will have to be updated if we change how constraints work in langkit
class InlineConstraintOptions(TypedDict):
    upper_threshold: NotRequired[Optional[float]]
    upper_threshold_inclusive: NotRequired[Optional[float]]
    lower_threshold: NotRequired[Optional[float]]
    lower_threshold_inclusive: NotRequired[Optional[float]]
    one_of: NotRequired[Optional[Sequence[Union[str, float, int]]]]
    none_of: NotRequired[Optional[Sequence[Union[str, float, int]]]]
    must_be_non_none: NotRequired[Optional[bool]]
    must_be_none: NotRequired[Optional[bool]]
    failure_level: NotRequired[Optional[ValidationFailureLevel]]


class ValidatorConverter:
    @staticmethod
    def to_langkit_options(validator: Validators) -> StrictLangkitOptions:
        if isinstance(validator, PolicyConstraintValidator):
            return StrictLangkitOptions(validators=[validation_lib.constraint(**validator.options)])
        else:
            return StrictLangkitOptions(validators=[validation_lib.multi_column_constraint(**validator.options)])


class BaseCallback(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    # callback: str
    # options: Optional[Mapping[str, Union[bool, str]]] = None  # Specific to the callback, can't be typed

    @field_validator("options", mode="after", check_fields=False)
    def hashable_dicts(cls, it: Any):
        if isinstance(it, HashableDict):
            return it

        if isinstance(it, list):
            return HashableList.from_list(it)  # pyright: ignore[reportUnknownArgumentType]

        if isinstance(it, dict):
            return HashableDict.from_dict(it)  # pyright: ignore[reportUnknownArgumentType]

        return it


class BasicValidationFailureCallback(BaseCallback):
    model_config = ConfigDict(frozen=True, extra="forbid")
    callback: Literal["webhook.basic_validation_failure"]
    options: BasicValidationFailureOptions = Field(json_schema_extra={"additionalProperties": False})


class StaticBearerAuthValidationFailureCallback(BaseCallback):
    model_config = ConfigDict(frozen=True, extra="forbid")
    callback: Literal["webhook.static_bearer_auth_validation_failure"]
    options: StaticBearerAuthValidationFailureOptions = Field(json_schema_extra={"additionalProperties": False})


class SlackValidationFailureCallback(BaseCallback):
    model_config = ConfigDict(frozen=True, extra="forbid")
    callback: Literal["webhook.slack_validation_failure"]
    options: SlackValidationFailureOptions = Field(json_schema_extra={"additionalProperties": False})


Callbacks = Union[BasicValidationFailureCallback, StaticBearerAuthValidationFailureCallback, SlackValidationFailureCallback]


class CallbackConverter:
    @staticmethod
    def to_langkit_options(callback: Callbacks) -> StrictLangkitOptions:
        callback_creator = callback_types.get(callback.callback)

        if callback_creator is None:
            raise ValueError(f"Unknown callback {callback.callback}. Pick one of {callback_types.keys()}")

        return StrictLangkitOptions(callbacks=[callback_creator(HashableDict(callback.options or {}))])


Behavior = Literal["observe", "flag", "block"]
Sensitivity = Literal["low", "medium", "high"]


class UtilRuleSetOptions(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    behavior: Behavior
    sensitivity: Sensitivity


class TruthfulnessRuleSetOptions(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    behavior: Behavior
    sensitivity: Sensitivity
    rag_enabled: bool
    hallucinations_enabled: bool


class PromptTruthfulnessRuleSet(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    ruleset: Literal["prompt.score.truthfulness"]
    options: TruthfulnessRuleSetOptions

    def to_langkit_options(self) -> StrictLangkitOptions:
        options = CachedRulesets.prompt_truthfulness(
            self.options.behavior,
            self.options.sensitivity,
            self.options.rag_enabled,
            self.options.hallucinations_enabled,
        )

        return StrictLangkitOptions(
            metrics=options.metrics,
            callbacks=options.callbacks,
            validators=options.validators,
        )


class ResponseTruthfulnessRuleSet(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    ruleset: Literal["response.score.truthfulness"]
    options: TruthfulnessRuleSetOptions

    def to_langkit_options(self) -> StrictLangkitOptions:
        options = CachedRulesets.response_truthfulness(
            self.options.behavior,
            self.options.sensitivity,
            self.options.rag_enabled,
            self.options.hallucinations_enabled,
        )

        return StrictLangkitOptions(
            metrics=options.metrics,
            callbacks=options.callbacks,
            validators=options.validators,
        )


class PromptUtilRuleSet(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    ruleset: Literal["prompt.score.util"]
    options: UtilRuleSetOptions

    def to_langkit_options(self) -> StrictLangkitOptions:
        options = CachedRulesets.prompt_util(self.options.behavior, self.options.sensitivity)

        return StrictLangkitOptions(
            metrics=options.metrics,
            callbacks=options.callbacks,
            validators=options.validators,
        )


class ResponseUtilRuleSet(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    ruleset: Literal["response.score.util"]
    options: UtilRuleSetOptions

    def to_langkit_options(self) -> StrictLangkitOptions:
        options = CachedRulesets.response_util(self.options.behavior, self.options.sensitivity)

        return StrictLangkitOptions(
            metrics=options.metrics,
            callbacks=options.callbacks,
            validators=options.validators,
        )


class CostRuleSetOptions(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    behavior: Behavior
    sensitivity: Sensitivity


class PromptCostRuleSet(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    ruleset: Literal["prompt.score.cost"]
    options: CostRuleSetOptions

    def to_langkit_options(self) -> StrictLangkitOptions:
        options = CachedRulesets.prompt_cost(self.options.behavior, self.options.sensitivity)

        return StrictLangkitOptions(
            metrics=options.metrics,
            callbacks=options.callbacks,
            validators=options.validators,
        )


class ResponseCostRuleSet(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    ruleset: Literal["response.score.cost"]
    options: CostRuleSetOptions

    def to_langkit_options(self) -> StrictLangkitOptions:
        options = CachedRulesets.response_cost(self.options.behavior, self.options.sensitivity)

        return StrictLangkitOptions(
            metrics=options.metrics,
            callbacks=options.callbacks,
            validators=options.validators,
        )


class CustomerExperienceRuleSetOptions(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    behavior: Behavior
    sensitivity: Sensitivity


class PromptCustomerExperienceRuleSet(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    ruleset: Literal["prompt.score.customer_experience"]
    options: CustomerExperienceRuleSetOptions

    def to_langkit_options(self) -> StrictLangkitOptions:
        options = CachedRulesets.prompt_customer_experience(self.options.behavior, self.options.sensitivity)

        return StrictLangkitOptions(
            metrics=options.metrics,
            callbacks=options.callbacks,
            validators=options.validators,
        )


class ResponseCustomerExperienceRuleSet(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    ruleset: Literal["response.score.customer_experience"]
    options: CustomerExperienceRuleSetOptions

    def to_langkit_options(self) -> StrictLangkitOptions:
        options = CachedRulesets.response_customer_experience(self.options.behavior, self.options.sensitivity)

        return StrictLangkitOptions(
            metrics=options.metrics,
            callbacks=options.callbacks,
            validators=options.validators,
        )


class BadActorsRuleSetOptions(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    behavior: Behavior
    sensitivity: Sensitivity


class PromptBadActorsRuleSet(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    ruleset: Literal["prompt.score.bad_actors"]
    options: BadActorsRuleSetOptions

    def to_langkit_options(self) -> StrictLangkitOptions:
        options = CachedRulesets.prompt_bad_actors(self.options.behavior, self.options.sensitivity)

        return StrictLangkitOptions(
            metrics=options.metrics,
            callbacks=options.callbacks,
            validators=options.validators,
        )


class ResponseBadActorsRuleSet(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    ruleset: Literal["response.score.bad_actors"]
    options: BadActorsRuleSetOptions

    def to_langkit_options(self) -> StrictLangkitOptions:
        options = CachedRulesets.response_bad_actors(self.options.behavior, self.options.sensitivity)

        return StrictLangkitOptions(
            metrics=options.metrics,
            callbacks=options.callbacks,
            validators=options.validators,
        )


class MisuseRuleSetOptions(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    behavior: Behavior
    sensitivity: Sensitivity
    topics: Tuple[str, ...]


class PromptMisuseRuleSet(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    ruleset: Literal["prompt.score.misuse"]
    options: MisuseRuleSetOptions

    def to_langkit_options(self) -> StrictLangkitOptions:
        options = CachedRulesets.prompt_misuse(self.options.behavior, self.options.sensitivity, self.options.topics)

        return StrictLangkitOptions(
            metrics=options.metrics,
            callbacks=options.callbacks,
            validators=options.validators,
        )


class ResponseMisuseRuleSet(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    ruleset: Literal["response.score.misuse"]
    options: MisuseRuleSetOptions

    def to_langkit_options(self) -> StrictLangkitOptions:
        options = CachedRulesets.response_misuse(self.options.behavior, self.options.sensitivity, self.options.topics)

        return StrictLangkitOptions(
            metrics=options.metrics,
            callbacks=options.callbacks,
            validators=options.validators,
        )


Rulesets = Union[
    PromptMisuseRuleSet,
    PromptBadActorsRuleSet,
    PromptCustomerExperienceRuleSet,
    PromptTruthfulnessRuleSet,
    PromptCostRuleSet,
    PromptUtilRuleSet,
    ResponseMisuseRuleSet,
    ResponseBadActorsRuleSet,
    ResponseCustomerExperienceRuleSet,
    ResponseTruthfulnessRuleSet,
    ResponseCostRuleSet,
    ResponseUtilRuleSet,
]


class MetaRulesetBaseModel(BaseModel):
    def to_rulesets(self) -> Sequence[Rulesets]: ...


class TruthfulnessRuleSet(MetaRulesetBaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    ruleset: Literal["score.truthfulness"]
    options: TruthfulnessRuleSetOptions

    def to_rulesets(self) -> Sequence[Rulesets]:
        return [
            PromptTruthfulnessRuleSet(ruleset="prompt.score.truthfulness", options=self.options),
            ResponseTruthfulnessRuleSet(ruleset="response.score.truthfulness", options=self.options),
        ]


class CustomerExperienceRuleSet(MetaRulesetBaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    ruleset: Literal["score.customer_experience"]
    options: CustomerExperienceRuleSetOptions

    def to_rulesets(self) -> Sequence[Rulesets]:
        return [
            PromptCustomerExperienceRuleSet(ruleset="prompt.score.customer_experience", options=self.options),
            ResponseCustomerExperienceRuleSet(ruleset="response.score.customer_experience", options=self.options),
        ]


class MisuseRuleSet(MetaRulesetBaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    ruleset: Literal["score.misuse"]
    options: MisuseRuleSetOptions

    def to_rulesets(self) -> Sequence[Rulesets]:
        return [
            PromptMisuseRuleSet(ruleset="prompt.score.misuse", options=self.options),
            ResponseMisuseRuleSet(ruleset="response.score.misuse", options=self.options),
        ]


class BadActorsRuleSet(MetaRulesetBaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    ruleset: Literal["score.bad_actors"]
    options: BadActorsRuleSetOptions

    def to_rulesets(self) -> Sequence[Rulesets]:
        return [
            PromptBadActorsRuleSet(ruleset="prompt.score.bad_actors", options=self.options),
            ResponseBadActorsRuleSet(ruleset="response.score.bad_actors", options=self.options),
        ]


class CostRuleSet(MetaRulesetBaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    ruleset: Literal["score.cost"]
    options: CostRuleSetOptions

    def to_rulesets(self) -> Sequence[Rulesets]:
        return [
            PromptCostRuleSet(ruleset="prompt.score.cost", options=self.options),
            ResponseCostRuleSet(ruleset="response.score.cost", options=self.options),
        ]


class UtilRuleSet(MetaRulesetBaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    ruleset: Literal["score.util"]
    options: UtilRuleSetOptions

    def to_rulesets(self) -> Sequence[Rulesets]:
        return [
            PromptUtilRuleSet(ruleset="prompt.score.util", options=self.options),
            ResponseUtilRuleSet(ruleset="response.score.util", options=self.options),
        ]


MetaRulesets = Union[
    MisuseRuleSet,
    BadActorsRuleSet,
    CustomerExperienceRuleSet,
    TruthfulnessRuleSet,
    CostRuleSet,
]


OptionsValueType = Union[object, float, int, str, bool, Tuple[Union[float, int, str, bool], ...]]

MappingType = Union[Mapping[str, OptionsValueType], Mapping[str, Mapping[str, OptionsValueType]]]

OptionsType = Optional[MappingType]


MetricOptions = TypeVar("MetricOptions", bound=MappingType)

MetricName = TypeVar("MetricName", bound=str)


class MetricProtocolFull(Protocol, Generic[MetricName, MetricOptions]):
    metric: MetricName
    options: MetricOptions
    profile: Optional[bool]


class MetricProtocolInlineValidation(Protocol, Generic[MetricName, MetricOptions]):
    metric: MetricName
    options: MetricOptions
    profile: Optional[bool]
    validation: Optional[InlineConstraintOptions]


class MetricProtocolOptionless(Protocol, Generic[MetricName]):
    metric: MetricName
    profile: Optional[bool]


MetricProtocol = Union[MetricProtocolFull[Any, Any], MetricProtocolOptionless[Any], MetricProtocolInlineValidation[Any, Any]]


class MetricConverter:
    @staticmethod
    def to_langkit_options(metric: MetricProtocol) -> StrictLangkitOptions:
        metric_name = metric.metric
        metric_creator = get_metric_names().get(metric_name)

        if metric_creator is None:
            raise ValueError(f"Unknown metric {metric_name}. Pick one of {get_metric_names().keys()}")

        validators: List[Validator] = []
        if hasattr(metric, "validation"):
            validation = cast(Optional[InlineConstraintOptions], metric.validation if metric.validation is not None else None)  # pyright: ignore[reportUnknownMemberType, reportAttributeAccessIssue]
            if validation is not None:
                one_of = validation.get("one_of")
                one_of = tuple(one_of) if one_of else None
                none_of = validation.get("none_of")
                none_of = tuple(none_of) if none_of else None

                validators.append(
                    ConstraintValidator(
                        ConstraintValidatorOptions(
                            target_metric=metric_name,
                            upper_threshold=validation.get("upper_threshold"),
                            upper_threshold_inclusive=validation.get("upper_threshold_inclusive"),
                            lower_threshold=validation.get("lower_threshold"),
                            lower_threshold_inclusive=validation.get("lower_threshold_inclusive"),
                            one_of=one_of,
                            none_of=none_of,
                            must_be_non_none=validation.get("must_be_non_none"),
                            must_be_none=validation.get("must_be_none"),
                            failure_level=validation.get("failure_level"),
                        )
                    )
                )

        if hasattr(metric, "options"):
            # If it exists, we can safely cast to MetricProtocol
            metric_with_options = cast(MetricProtocolFull[Any, Any], metric)
            options = metric_with_options.options
        else:
            options = None

        metrics = metric_creator(HashableDict(options or {}))
        return StrictLangkitOptions(metrics=[lambda: metrics], validators=validators)


class BaseMetric(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", arbitrary_types_allowed=True)

    @field_validator("options", "validation", mode="after", check_fields=False)
    def hashable_dicts(cls, it: Any):
        if isinstance(it, HashableDict):
            return it

        if isinstance(it, list):
            return HashableList.from_list(it)  # pyright: ignore[reportUnknownArgumentType]

        if isinstance(it, dict):
            return HashableDict.from_dict(it)  # pyright: ignore[reportUnknownArgumentType]

        return it


# Presets
class PresetsAllMetric(BaseMetric):
    model_config = ConfigDict(frozen=True, extra="forbid")
    metric: Literal["presets.all"]
    options: PresetOptions = Field(HashableDict.from_dict({}), json_schema_extra={"additionalProperties": False})
    profile: Optional[bool] = True


class PresetsRecommendedMetric(BaseMetric):
    model_config = ConfigDict(frozen=True, extra="forbid")
    metric: Literal["presets.recommended"]
    options: PresetOptions = Field(HashableDict.from_dict({}), json_schema_extra={"additionalProperties": False})
    profile: Optional[bool] = True


# CUSTOM_COLUMN similarity
class CustomColumnSimilarityMetric(BaseMetric):
    model_config = ConfigDict(frozen=True, extra="forbid")
    metric: Literal["CUSTOM_COLUMN.similarity.CUSTOM_COLUMN_2"]
    # This is a typeddict
    options: CustomColumnSimilarityOptions = Field(description="This is a typeddict", json_schema_extra={"additionalProperties": False})
    profile: Optional[bool] = True


# Prompt metrics
class PromptTokenCountMetric(BaseMetric):
    model_config = ConfigDict(frozen=True, extra="forbid")
    metric: Literal["prompt.stats.token_count"]
    options: TokenCountOptions = Field(HashableDict.from_dict({}), json_schema_extra={"additionalProperties": False})
    profile: Optional[bool] = True
    validation: Optional[InlineConstraintOptions] = Field(None, json_schema_extra={"additionalProperties": False})


class PromptCharCountMetric(BaseMetric):
    model_config = ConfigDict(frozen=True, extra="forbid")
    metric: Literal["prompt.stats.char_count"]
    profile: Optional[bool] = True
    validation: Optional[InlineConstraintOptions] = Field(None, json_schema_extra={"additionalProperties": False})


class PromptFleschReadingEaseMetric(BaseMetric):
    model_config = ConfigDict(frozen=True, extra="forbid")
    metric: Literal["prompt.stats.flesch_reading_ease"]
    profile: Optional[bool] = True
    validation: Optional[InlineConstraintOptions] = Field(None, json_schema_extra={"additionalProperties": False})


class PromptSentenceCountMetric(BaseMetric):
    model_config = ConfigDict(frozen=True, extra="forbid")
    metric: Literal["prompt.stats.sentence_count"]
    profile: Optional[bool] = True
    validation: Optional[InlineConstraintOptions] = Field(None, json_schema_extra={"additionalProperties": False})


class PromptGradeMetric(BaseMetric):
    model_config = ConfigDict(frozen=True, extra="forbid")
    metric: Literal["prompt.stats.grade"]
    profile: Optional[bool] = True
    validation: Optional[InlineConstraintOptions] = Field(None, json_schema_extra={"additionalProperties": False})


class PromptLexiconCountMetric(BaseMetric):
    model_config = ConfigDict(frozen=True, extra="forbid")
    metric: Literal["prompt.stats.lexicon_count"]
    profile: Optional[bool] = True
    validation: Optional[InlineConstraintOptions] = Field(None, json_schema_extra={"additionalProperties": False})


class PromptLetterCountMetric(BaseMetric):
    model_config = ConfigDict(frozen=True, extra="forbid")
    metric: Literal["prompt.stats.letter_count"]
    profile: Optional[bool] = True
    validation: Optional[InlineConstraintOptions] = Field(None, json_schema_extra={"additionalProperties": False})


class PromptDifficultWordsMetric(BaseMetric):
    model_config = ConfigDict(frozen=True, extra="forbid")
    metric: Literal["prompt.stats.difficult_words"]
    profile: Optional[bool] = True
    validation: Optional[InlineConstraintOptions] = Field(None, json_schema_extra={"additionalProperties": False})


# Prompt regex metrics
class PromptRegexMetric(BaseMetric):
    model_config = ConfigDict(frozen=True, extra="forbid")
    metric: Literal["prompt.regex"]
    profile: Optional[bool] = True


class PromptRegexSSNMetric(BaseMetric):
    model_config = ConfigDict(frozen=True, extra="forbid")
    metric: Literal["prompt.regex.ssn"]
    profile: Optional[bool] = True
    validation: Optional[InlineConstraintOptions] = Field(None, json_schema_extra={"additionalProperties": False})


class PromptRegexPhoneNumberMetric(BaseMetric):
    model_config = ConfigDict(frozen=True, extra="forbid")
    metric: Literal["prompt.regex.phone_number"]
    profile: Optional[bool] = True
    validation: Optional[InlineConstraintOptions] = Field(None, json_schema_extra={"additionalProperties": False})


class PromptRegexEmailAddressMetric(BaseMetric):
    model_config = ConfigDict(frozen=True, extra="forbid")
    metric: Literal["prompt.regex.email_address"]
    profile: Optional[bool] = True
    validation: Optional[InlineConstraintOptions] = Field(None, json_schema_extra={"additionalProperties": False})


class PromptRegexMailingAddressMetric(BaseMetric):
    model_config = ConfigDict(frozen=True, extra="forbid")
    metric: Literal["prompt.regex.mailing_address"]
    profile: Optional[bool] = True
    validation: Optional[InlineConstraintOptions] = Field(None, json_schema_extra={"additionalProperties": False})


class PromptRegexCreditCardNumberMetric(BaseMetric):
    model_config = ConfigDict(frozen=True, extra="forbid")
    metric: Literal["prompt.regex.credit_card_number"]
    profile: Optional[bool] = True
    validation: Optional[InlineConstraintOptions] = Field(None, json_schema_extra={"additionalProperties": False})


class PromptRegexURLMetric(BaseMetric):
    model_config = ConfigDict(frozen=True, extra="forbid")
    metric: Literal["prompt.regex.url"]
    profile: Optional[bool] = True
    validation: Optional[InlineConstraintOptions] = Field(None, json_schema_extra={"additionalProperties": False})


class PromptPiiMetric(BaseMetric):
    model_config = ConfigDict(frozen=True, extra="forbid")
    metric: Literal["prompt.pii"]
    options: PiiOptions = Field(HashableDict.from_dict({}), json_schema_extra={"additionalProperties": False})
    profile: Optional[bool] = True


class PromptSimilarityMetric(BaseMetric):
    model_config = ConfigDict(frozen=True, extra="forbid")
    metric: Literal["prompt.similarity"]
    profile: Optional[bool] = True


class PromptSimilarityContextMetric(BaseMetric):
    model_config = ConfigDict(frozen=True, extra="forbid")
    metric: Literal["prompt.similarity.context"]
    options: EmbeddingOptions = Field(HashableDict.from_dict({}), json_schema_extra={"additionalProperties": False})
    profile: Optional[bool] = True
    validation: Optional[InlineConstraintOptions] = Field(None, json_schema_extra={"additionalProperties": False})


class PromptSimilarityCustomColumnMetric(BaseMetric):
    model_config = ConfigDict(frozen=True, extra="forbid")
    metric: Literal["prompt.similarity.CUSTOM_COLUMN"]
    options: PromptSimilarityCustomColumnOptions = Field(json_schema_extra={"additionalProperties": False})
    profile: Optional[bool] = True


class InjectionMetric(BaseMetric):
    model_config = ConfigDict(frozen=True, extra="forbid")
    metric: Literal["prompt.similarity.injection"]
    options: InjectionOptions = Field(HashableDict.from_dict({}), json_schema_extra={"additionalProperties": False})
    profile: Optional[bool] = True
    validation: Optional[InlineConstraintOptions] = Field(None, json_schema_extra={"additionalProperties": False})


class PromptSentimentMetric(BaseMetric):
    model_config = ConfigDict(frozen=True, extra="forbid")
    metric: Literal["prompt.sentiment"]
    profile: Optional[bool] = True


class PromptSentimentScoreMetric(BaseMetric):
    model_config = ConfigDict(frozen=True, extra="forbid")
    metric: Literal["prompt.sentiment.sentiment_score"]
    profile: Optional[bool] = True
    validation: Optional[InlineConstraintOptions] = Field(None, json_schema_extra={"additionalProperties": False})


class PromptToxicityMetric(BaseMetric):
    model_config = ConfigDict(frozen=True, extra="forbid")
    metric: Literal["prompt.toxicity"]
    profile: Optional[bool] = True


class PromptToxicityScoreMetric(BaseMetric):
    model_config = ConfigDict(frozen=True, extra="forbid")
    metric: Literal["prompt.toxicity.toxicity_score"]
    options: ToxicityScoreOptions = Field(HashableDict.from_dict({}), json_schema_extra={"additionalProperties": False})
    profile: Optional[bool] = True
    validation: Optional[InlineConstraintOptions] = Field(None, json_schema_extra={"additionalProperties": False})


class PromptUtilEmbeddingsMetric(BaseMetric):
    model_config = ConfigDict(frozen=True, extra="forbid")
    metric: Literal["prompt.util.embeddings"]
    options: EmbeddingOptions = Field(HashableDict.from_dict({}), json_schema_extra={"additionalProperties": False})
    profile: Optional[bool] = True


class PromptPcaCoordinatesMetric(BaseMetric):
    model_config = ConfigDict(frozen=True, extra="forbid")
    metric: Literal["prompt.pca.coordinates"]
    options: CoordinatesOptions = Field(HashableDict.from_dict({}), json_schema_extra={"additionalProperties": False})
    profile: Optional[bool] = True


# Prompt topics
class PromptTopicsMetric(BaseMetric):
    model_config = ConfigDict(frozen=True, extra="forbid")
    metric: Literal["prompt.topics"]
    options: TopicsOptions = Field(json_schema_extra={"additionalProperties": False})
    profile: Optional[bool] = True


class PromptTopicsMedicalMetric(BaseMetric):
    model_config = ConfigDict(frozen=True, extra="forbid")
    metric: Literal["prompt.topics.medical"]
    profile: Optional[bool] = True
    validation: Optional[InlineConstraintOptions] = Field(None, json_schema_extra={"additionalProperties": False})


class PromptTopicsFinancialMetric(BaseMetric):
    model_config = ConfigDict(frozen=True, extra="forbid")
    metric: Literal["prompt.topics.financial"]
    profile: Optional[bool] = True
    validation: Optional[InlineConstraintOptions] = Field(None, json_schema_extra={"additionalProperties": False})


class PromptTopicsCodeMetric(BaseMetric):
    model_config = ConfigDict(frozen=True, extra="forbid")
    metric: Literal["prompt.topics.code"]
    profile: Optional[bool] = True
    validation: Optional[InlineConstraintOptions] = Field(None, json_schema_extra={"additionalProperties": False})


class PromptTopicsMaliciousMetric(BaseMetric):
    model_config = ConfigDict(frozen=True, extra="forbid")
    metric: Literal["prompt.topics.malicious"]
    profile: Optional[bool] = True
    validation: Optional[InlineConstraintOptions] = Field(None, json_schema_extra={"additionalProperties": False})


# Response metrics
class ResponseTokenCountMetric(BaseMetric):
    model_config = ConfigDict(frozen=True, extra="forbid")
    metric: Literal["response.stats.token_count"]
    options: TokenCountOptions = Field(HashableDict.from_dict({}), json_schema_extra={"additionalProperties": False})
    profile: Optional[bool] = True
    validation: Optional[InlineConstraintOptions] = Field(None, json_schema_extra={"additionalProperties": False})


class ResponseCharCountMetric(BaseMetric):
    model_config = ConfigDict(frozen=True, extra="forbid")
    metric: Literal["response.stats.char_count"]
    profile: Optional[bool] = True
    validation: Optional[InlineConstraintOptions] = Field(None, json_schema_extra={"additionalProperties": False})


class ResponseFleschReadingEaseMetric(BaseMetric):
    model_config = ConfigDict(frozen=True, extra="forbid")
    metric: Literal["response.stats.flesch_reading_ease"]
    profile: Optional[bool] = True
    validation: Optional[InlineConstraintOptions] = Field(None, json_schema_extra={"additionalProperties": False})


class ResponseSentenceCountMetric(BaseMetric):
    model_config = ConfigDict(frozen=True, extra="forbid")
    metric: Literal["response.stats.sentence_count"]
    profile: Optional[bool] = True
    validation: Optional[InlineConstraintOptions] = Field(None, json_schema_extra={"additionalProperties": False})


class ResponseGradeMetric(BaseMetric):
    model_config = ConfigDict(frozen=True, extra="forbid")
    metric: Literal["response.stats.grade"]
    profile: Optional[bool] = True
    validation: Optional[InlineConstraintOptions] = Field(None, json_schema_extra={"additionalProperties": False})


class ResponseLexiconCountMetric(BaseMetric):
    model_config = ConfigDict(frozen=True, extra="forbid")
    metric: Literal["response.stats.lexicon_count"]
    profile: Optional[bool] = True
    validation: Optional[InlineConstraintOptions] = Field(None, json_schema_extra={"additionalProperties": False})


class ResponseLetterCountMetric(BaseMetric):
    model_config = ConfigDict(frozen=True, extra="forbid")
    metric: Literal["response.stats.letter_count"]
    profile: Optional[bool] = True
    validation: Optional[InlineConstraintOptions] = Field(None, json_schema_extra={"additionalProperties": False})


class ResponseDifficultWordsMetric(BaseMetric):
    model_config = ConfigDict(frozen=True, extra="forbid")
    metric: Literal["response.stats.difficult_words"]
    profile: Optional[bool] = True
    validation: Optional[InlineConstraintOptions] = Field(None, json_schema_extra={"additionalProperties": False})


# Response regex metrics
class ResponseRegexMetric(BaseMetric):
    model_config = ConfigDict(frozen=True, extra="forbid")
    metric: Literal["response.regex"]
    profile: Optional[bool] = True


class ResponseRegexRefusalMetric(BaseMetric):
    model_config = ConfigDict(frozen=True, extra="forbid")
    metric: Literal["response.regex.refusal"]
    profile: Optional[bool] = True
    validation: Optional[InlineConstraintOptions] = Field(None, json_schema_extra={"additionalProperties": False})


class ResponseRegexSSNMetric(BaseMetric):
    model_config = ConfigDict(frozen=True, extra="forbid")
    metric: Literal["response.regex.ssn"]
    profile: Optional[bool] = True
    validation: Optional[InlineConstraintOptions] = Field(None, json_schema_extra={"additionalProperties": False})


class ResponseRegexPhoneNumberMetric(BaseMetric):
    model_config = ConfigDict(frozen=True, extra="forbid")
    metric: Literal["response.regex.phone_number"]
    profile: Optional[bool] = True
    validation: Optional[InlineConstraintOptions] = Field(None, json_schema_extra={"additionalProperties": False})


class ResponseRegexEmailAddressMetric(BaseMetric):
    model_config = ConfigDict(frozen=True, extra="forbid")
    metric: Literal["response.regex.email_address"]
    profile: Optional[bool] = True
    validation: Optional[InlineConstraintOptions] = Field(None, json_schema_extra={"additionalProperties": False})


class ResponseRegexMailingAddressMetric(BaseMetric):
    model_config = ConfigDict(frozen=True, extra="forbid")
    metric: Literal["response.regex.mailing_address"]
    profile: Optional[bool] = True
    validation: Optional[InlineConstraintOptions] = Field(None, json_schema_extra={"additionalProperties": False})


class ResponseRegexCreditCardNumberMetric(BaseMetric):
    model_config = ConfigDict(frozen=True, extra="forbid")
    metric: Literal["response.regex.credit_card_number"]
    profile: Optional[bool] = True
    validation: Optional[InlineConstraintOptions] = Field(None, json_schema_extra={"additionalProperties": False})


class ResponseRegexURLMetric(BaseMetric):
    model_config = ConfigDict(frozen=True, extra="forbid")
    metric: Literal["response.regex.url"]
    profile: Optional[bool] = True
    validation: Optional[InlineConstraintOptions] = Field(None, json_schema_extra={"additionalProperties": False})


class ResponsePiiMetric(BaseMetric):
    model_config = ConfigDict(frozen=True, extra="forbid")
    metric: Literal["response.pii"]
    options: PiiOptions = Field(HashableDict.from_dict({}), json_schema_extra={"additionalProperties": False})
    profile: Optional[bool] = True


class ResponseSimilarityMetric(BaseMetric):
    model_config = ConfigDict(frozen=True, extra="forbid")
    metric: Literal["response.similarity"]
    profile: Optional[bool] = True


class ResponseSimilarityRefusalMetric(BaseMetric):
    model_config = ConfigDict(frozen=True, extra="forbid")
    metric: Literal["response.similarity.refusal"]
    options: RefusalOptions = Field(HashableDict.from_dict({}), json_schema_extra={"additionalProperties": False})
    profile: Optional[bool] = True
    validation: Optional[InlineConstraintOptions] = Field(None, json_schema_extra={"additionalProperties": False})


class ResponseSimilarityPromptMetric(BaseMetric):
    model_config = ConfigDict(frozen=True, extra="forbid")
    metric: Literal["response.similarity.prompt"]
    options: EmbeddingOptions = Field(HashableDict.from_dict({}), json_schema_extra={"additionalProperties": False})
    profile: Optional[bool] = True
    validation: Optional[InlineConstraintOptions] = Field(None, json_schema_extra={"additionalProperties": False})


class ResponseSimilarityCustomColumnMetric(BaseMetric):
    model_config = ConfigDict(frozen=True, extra="forbid")
    metric: Literal["response.similarity.CUSTOM_COLUMN"]
    options: ResponseSimilarityCustomColumnOptions = Field(json_schema_extra={"additionalProperties": False})
    profile: Optional[bool] = True


class ResponseSimilarityContextMetric(BaseMetric):
    model_config = ConfigDict(frozen=True, extra="forbid")
    metric: Literal["response.similarity.context"]
    options: EmbeddingOptions = Field(HashableDict.from_dict({}), json_schema_extra={"additionalProperties": False})
    profile: Optional[bool] = True
    validation: Optional[InlineConstraintOptions] = Field(None, json_schema_extra={"additionalProperties": False})


class ResponseSentimentMetric(BaseMetric):
    model_config = ConfigDict(frozen=True, extra="forbid")
    metric: Literal["response.sentiment"]
    profile: Optional[bool] = True


class ResponseSentimentScoreMetric(BaseMetric):
    model_config = ConfigDict(frozen=True, extra="forbid")
    metric: Literal["response.sentiment.sentiment_score"]
    profile: Optional[bool] = True
    validation: Optional[InlineConstraintOptions] = Field(None, json_schema_extra={"additionalProperties": False})


class ResponseToxicityMetric(BaseMetric):
    model_config = ConfigDict(frozen=True, extra="forbid")
    metric: Literal["response.toxicity"]
    profile: Optional[bool] = True


class ResponseToxicityScoreMetric(BaseMetric):
    model_config = ConfigDict(frozen=True, extra="forbid")
    metric: Literal["response.toxicity.toxicity_score"]
    options: ToxicityScoreOptions = Field(HashableDict.from_dict({}), json_schema_extra={"additionalProperties": False})
    profile: Optional[bool] = True
    validation: Optional[InlineConstraintOptions] = Field(None, json_schema_extra={"additionalProperties": False})


class ResponseUtilEmbeddingsMetric(BaseMetric):
    model_config = ConfigDict(frozen=True, extra="forbid")
    metric: Literal["response.util.embeddings"]
    options: EmbeddingOptions = Field(HashableDict.from_dict({}), json_schema_extra={"additionalProperties": False})
    profile: Optional[bool] = True
    validation: Optional[InlineConstraintOptions] = Field(None, json_schema_extra={"additionalProperties": False})


class ResponsePcaCoordinatesMetric(BaseMetric):
    model_config = ConfigDict(frozen=True, extra="forbid")
    metric: Literal["response.pca.coordinates"]
    options: CoordinatesOptions = Field(HashableDict.from_dict({}), json_schema_extra={"additionalProperties": False})
    profile: Optional[bool] = True
    validation: Optional[InlineConstraintOptions] = Field(None, json_schema_extra={"additionalProperties": False})


class ResponseHallucinationScoreMetric(BaseMetric):
    model_config = ConfigDict(frozen=True, extra="forbid")
    metric: Literal["response.hallucination.hallucination_score"]
    options: HallucinationScoreOptions = Field(HashableDict.from_dict({}), json_schema_extra={"additionalProperties": False})
    profile: Optional[bool] = True
    validation: Optional[InlineConstraintOptions] = Field(None, json_schema_extra={"additionalProperties": False})


# Response topics
class ResponseTopicsMetric(BaseMetric):
    model_config = ConfigDict(frozen=True, extra="forbid")
    metric: Literal["response.topics"]
    options: TopicsOptions = Field(json_schema_extra={"additionalProperties": False})
    profile: Optional[bool] = True


class ResponseTopicsMedicalMetric(BaseMetric):
    model_config = ConfigDict(frozen=True, extra="forbid")
    metric: Literal["response.topics.medical"]
    profile: Optional[bool] = True
    validation: Optional[InlineConstraintOptions] = Field(None, json_schema_extra={"additionalProperties": False})


class ResponseTopicsFinancialMetric(BaseMetric):
    model_config = ConfigDict(frozen=True, extra="forbid")
    metric: Literal["response.topics.financial"]
    profile: Optional[bool] = True
    validation: Optional[InlineConstraintOptions] = Field(None, json_schema_extra={"additionalProperties": False})


class ResponseTopicsCodeMetric(BaseMetric):
    model_config = ConfigDict(frozen=True, extra="forbid")
    metric: Literal["response.topics.code"]
    profile: Optional[bool] = True
    validation: Optional[InlineConstraintOptions] = Field(None, json_schema_extra={"additionalProperties": False})


class ResponseTopicsMaliciousMetric(BaseMetric):
    model_config = ConfigDict(frozen=True, extra="forbid")
    metric: Literal["response.topics.malicious"]
    profile: Optional[bool] = True
    validation: Optional[InlineConstraintOptions] = Field(None, json_schema_extra={"additionalProperties": False})


Metrics = Union[
    PresetsAllMetric,
    PresetsRecommendedMetric,
    CustomColumnSimilarityMetric,
    PromptTokenCountMetric,
    PromptCharCountMetric,
    PromptFleschReadingEaseMetric,
    PromptSentenceCountMetric,
    PromptGradeMetric,
    PromptLexiconCountMetric,
    PromptLetterCountMetric,
    PromptDifficultWordsMetric,
    PromptRegexMetric,
    PromptRegexSSNMetric,
    PromptRegexPhoneNumberMetric,
    PromptRegexEmailAddressMetric,
    PromptRegexMailingAddressMetric,
    PromptRegexCreditCardNumberMetric,
    PromptRegexURLMetric,
    PromptPiiMetric,
    PromptSimilarityMetric,
    PromptSimilarityContextMetric,
    PromptSimilarityCustomColumnMetric,
    InjectionMetric,
    PromptSentimentMetric,
    PromptSentimentScoreMetric,
    PromptToxicityMetric,
    PromptToxicityScoreMetric,
    PromptUtilEmbeddingsMetric,
    PromptPcaCoordinatesMetric,
    PromptTopicsMetric,
    PromptTopicsMedicalMetric,
    PromptTopicsFinancialMetric,
    PromptTopicsCodeMetric,
    PromptTopicsMaliciousMetric,
    ResponseTokenCountMetric,
    ResponseCharCountMetric,
    ResponseFleschReadingEaseMetric,
    ResponseSentenceCountMetric,
    ResponseGradeMetric,
    ResponseLexiconCountMetric,
    ResponseLetterCountMetric,
    ResponseDifficultWordsMetric,
    ResponseRegexMetric,
    ResponseRegexRefusalMetric,
    ResponseRegexSSNMetric,
    ResponseRegexPhoneNumberMetric,
    ResponseRegexEmailAddressMetric,
    ResponseRegexMailingAddressMetric,
    ResponseRegexCreditCardNumberMetric,
    ResponseRegexURLMetric,
    ResponsePiiMetric,
    ResponseSimilarityMetric,
    ResponseSimilarityRefusalMetric,
    ResponseSimilarityPromptMetric,
    ResponseSimilarityCustomColumnMetric,
    ResponseSimilarityContextMetric,
    ResponseSentimentMetric,
    ResponseSentimentScoreMetric,
    ResponseToxicityMetric,
    ResponseToxicityScoreMetric,
    ResponseUtilEmbeddingsMetric,
    ResponsePcaCoordinatesMetric,
    ResponseHallucinationScoreMetric,
    ResponseTopicsMetric,
    ResponseTopicsMedicalMetric,
    ResponseTopicsFinancialMetric,
    ResponseTopicsCodeMetric,
    ResponseTopicsMaliciousMetric,
]


def combine_langkit_options(l1: StrictLangkitOptions, l2: StrictLangkitOptions) -> StrictLangkitOptions:
    return StrictLangkitOptions(
        metrics=l1.metrics + l2.metrics,
        callbacks=l1.callbacks + l2.callbacks,
        validators=l1.validators + l2.validators,
    )


class ActionOptions(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    block_message: Optional[str] = None
    flag_message: Optional[str] = None


class Policy(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    whylabs_dataset_id: str
    id: str
    # org_id has to be defined after whylabs_dataset_id so that it can be referenced in the pydantic field_validator
    org_id: str

    policy_version: int
    schema_version: Literal["0.1.0"]
    metrics: Optional[Tuple[Metrics, ...]] = Field(default_factory=tuple)
    callbacks: Optional[Tuple[Callbacks, ...]] = Field(default_factory=tuple)
    validators: Optional[Tuple[Validators, ...]] = Field(default_factory=tuple)
    rulesets: Optional[Tuple[Union[MetaRulesets, Rulesets], ...]] = Field(default_factory=tuple)
    actions: Optional[ActionOptions] = Field(default=None)

    def get_rulesets(self) -> Sequence[Rulesets]:
        rulesets: List[Rulesets] = []

        for ruleset in self.rulesets or tuple():
            if isinstance(ruleset, MetaRulesetBaseModel):
                rulesets.extend(ruleset.to_rulesets())
            else:
                rulesets.append(ruleset)

        # If there are any rulesets then implicitly include the util rulesets too
        if len(rulesets) > 0:
            rulesets.append(
                PromptUtilRuleSet(ruleset="prompt.score.util", options=UtilRuleSetOptions(behavior="observe", sensitivity="low"))
            )
            rulesets.append(
                ResponseUtilRuleSet(ruleset="response.score.util", options=UtilRuleSetOptions(behavior="observe", sensitivity="low"))
            )

        return rulesets

    def to_langkit_options(self) -> StrictLangkitOptions:
        # ensure no repeated in the rulesets section
        ruleset_names = [ruleset.ruleset for ruleset in self.rulesets or tuple()]
        if len(ruleset_names) != len(set(ruleset_names)):
            repeat_name = [name for name in ruleset_names if ruleset_names.count(name) > 1]
            raise ValueError(f"Repeated rulesets {set(repeat_name)} in policy {self.id} for dataset {self.whylabs_dataset_id}")

        try:
            metrics = [MetricConverter.to_langkit_options(metric) for metric in self.metrics or tuple()]
            callback = [CallbackConverter.to_langkit_options(callback) for callback in self.callbacks or tuple()]
            validators = [ValidatorConverter.to_langkit_options(validator) for validator in self.validators or tuple()]

            rulesets: List[StrictLangkitOptions] = [ruleset.to_langkit_options() for ruleset in self.get_rulesets()]

            return reduce(combine_langkit_options, metrics + callback + validators + rulesets, StrictLangkitOptions())
        except Exception as e:
            raise ValueError(f"Error while parsing policy {self.id} for dataset {self.whylabs_dataset_id}: {e}")

    @field_validator("whylabs_dataset_id", "id", mode="before")
    def ensure_str(cls, it: Any):
        """
        Ensure that these fields are parsed as strings. The python yaml parser sometimes pickes ints
        or floats if these are defined as something like `1`.
        """
        if not isinstance(it, str):
            return str(it)

        return it
