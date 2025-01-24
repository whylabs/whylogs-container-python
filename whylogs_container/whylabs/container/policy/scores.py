from functools import partial
from typing import Dict, List, Mapping, Optional, Sequence, Union

import pandas as pd
from whylabs_llm_toolkit.rulesets.normalize_scores import calculate_rule_set_score

from langkit.core.metric import MultiMetric, MultiMetricResult
from langkit.core.validation import ValidationFailureLevel, Validator
from langkit.core.workflow import Workflow
from langkit.validators.library import lib as validator_lib
from whylogs_container.whylabs.container.policy import Rulesets
from whylogs_container.whylabs.container.strict_container_types import StrictLangkitOptions


def _ruleset_udf(text: pd.DataFrame, ruleset_metric_name_map: Dict[str, Sequence[str]]) -> MultiMetricResult:
    """
    Calculate the score for a set of rulesets
    """
    rows: List[Dict[str, Union[int, float, Optional[str]]]] = [
        row.to_dict()  # pyright: ignore[reportUnknownMemberType]
        for _, row in text.iterrows()
    ]
    scores: Dict[str, List[Optional[int]]] = {}
    for ruleset_name, sub_metrics in ruleset_metric_name_map.items():
        scores[ruleset_name] = []
        for metric in sub_metrics:
            scores[metric] = []

    for ruleset_name in ruleset_metric_name_map:
        selected_metrics = ruleset_metric_name_map[ruleset_name]

        for row_dict in rows:
            row_projection = {k: row_dict[k] for k in selected_metrics if k in row_dict}
            score, all_scores = calculate_rule_set_score(row_projection)

            scores[ruleset_name].append(score)
            for metric, metric_score in all_scores.items():
                scores[metric].append(metric_score)

    expected_result_count = len(text)
    # update all of the entries that are still [] to be [None * expected_result_count]
    # We always return results for every score, so if some of the inputs were missing then we return
    # None for the outputs that needed them.
    for key, value in scores.items():
        if value == []:
            scores[key] = [None] * expected_result_count

    return MultiMetricResult(metrics=list(scores.values()))


def _score_metric(rulesets: Mapping[str, StrictLangkitOptions]) -> MultiMetric:
    # Get the names of all of the metrics that we're going to be scoring
    metrics: Dict[str, Sequence[str]] = {}
    output_metric_names: List[str] = []
    for ruleset, options in rulesets.items():
        metric_names = Workflow(metrics=options.metrics, lazy_init=True, cache_assets=False).get_metric_names()
        if not metric_names:
            # Some rulesets might not have any metrics, like prompt truthfulness. Its valid to request it but
            # there's nothing to do for it.
            continue
        metrics[ruleset] = metric_names
        output_metric_names.append(ruleset)
        for metric in metric_names:
            output_metric_names.append(f"{ruleset}.{metric}")

    def udf(text: pd.DataFrame) -> MultiMetricResult:
        return _ruleset_udf(text, metrics)

    # Doesn't mention any input names because it always runs and handles whatever inputs are available, rather
    # than depending on the workflow framework to run it only when all inputs are present.
    return MultiMetric(names=output_metric_names, input_names=[], evaluate=udf)


def _get_score_validators(rulesets: Sequence[Rulesets]) -> List[Validator]:
    validators: List[Validator] = []

    for ruleset in rulesets:
        if ruleset.options.behavior == "block":
            failure_level = "block"
        elif ruleset.options.behavior == "flag":
            failure_level: Optional[ValidationFailureLevel] = "flag"
        else:
            # If we're only observing then don't generate any validation failures
            continue

        if ruleset.options.sensitivity == "high":
            validators.append(
                validator_lib.constraint(target_metric=ruleset.ruleset, upper_threshold_inclusive=33, failure_level=failure_level)
            )
        elif ruleset.options.sensitivity == "medium":
            validators.append(
                validator_lib.constraint(target_metric=ruleset.ruleset, upper_threshold_inclusive=50, failure_level=failure_level)
            )
        elif ruleset.options.sensitivity == "low":
            validators.append(
                validator_lib.constraint(target_metric=ruleset.ruleset, upper_threshold_inclusive=66, failure_level=failure_level)
            )

    return validators


def create_score_workflow(rulesets: Sequence[Rulesets]) -> Workflow:
    ruleset_options: Dict[str, StrictLangkitOptions] = {}
    for ruleset in rulesets:
        options = ruleset.to_langkit_options()
        # filter out the ones that don't have any associated metrics. Truthfulness doesn't have metrics for prompt, for example
        if options.metrics:
            ruleset_options[ruleset.ruleset] = options

    # only take the rulesets that are present in the ruleset_dict keys
    filtered_rulesets = [ruleset for ruleset in rulesets if ruleset.ruleset in ruleset_options]
    return Workflow(metrics=[partial(_score_metric, ruleset_options)], validators=_get_score_validators(filtered_rulesets))
