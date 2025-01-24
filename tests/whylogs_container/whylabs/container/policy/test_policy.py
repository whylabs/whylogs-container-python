from typing import List, cast

from langkit.core.workflow import Workflow
from whylogs_container.whylabs.container.metrics.cached_metrics import CachedRulesets, get_metric_names
from whylogs_container.whylabs.container.policy.policy_0_0_1 import (
    BadActorsRuleSetOptions,
    CostRuleSetOptions,
    CustomerExperienceRuleSetOptions,
    MisuseRuleSetOptions,
    PromptBadActorsRuleSet,
    PromptCostRuleSet,
    PromptCustomerExperienceRuleSet,
    PromptMisuseRuleSet,
    PromptTruthfulnessRuleSet,
    ResponseBadActorsRuleSet,
    ResponseCostRuleSet,
    ResponseCustomerExperienceRuleSet,
    ResponseMisuseRuleSet,
    ResponseTruthfulnessRuleSet,
    TruthfulnessRuleSetOptions,
)
from whylogs_container.whylabs.container.policy.scores import create_score_workflow


def test_all_metrics():
    # This test etxists mostly to enforce that we're correctly reporting the full metric list in our docs via docsub insertion

    # DOCSUB_START all_metrics
    all_metrics = [
        "CUSTOM_COLUMN.similarity.CUSTOM_COLUMN_2",
        "presets.all",
        "presets.recommended",
        "prompt.pca.coordinates",
        "prompt.pii",
        "prompt.regex",
        "prompt.regex.credit_card_number",
        "prompt.regex.email_address",
        "prompt.regex.mailing_address",
        "prompt.regex.phone_number",
        "prompt.regex.ssn",
        "prompt.regex.url",
        "prompt.sentiment",
        "prompt.sentiment.sentiment_score",
        "prompt.similarity",
        "prompt.similarity.CUSTOM_COLUMN",
        "prompt.similarity.code",
        "prompt.similarity.context",
        "prompt.similarity.financial",
        "prompt.similarity.hate",
        "prompt.similarity.injection",
        "prompt.similarity.innocuous",
        "prompt.similarity.medical",
        "prompt.similarity.toxic",
        "prompt.stats",
        "prompt.stats.char_count",
        "prompt.stats.difficult_words",
        "prompt.stats.flesch_reading_ease",
        "prompt.stats.grade",
        "prompt.stats.letter_count",
        "prompt.stats.lexicon_count",
        "prompt.stats.sentence_count",
        "prompt.stats.token_count",
        "prompt.text_stat",
        "prompt.topics",
        "prompt.toxicity",
        "prompt.toxicity.toxicity_score",
        "prompt.util.embeddings",
        "response.hallucination.hallucination_score",
        "response.pca.coordinates",
        "response.pii",
        "response.regex",
        "response.regex.credit_card_number",
        "response.regex.email_address",
        "response.regex.mailing_address",
        "response.regex.phone_number",
        "response.regex.refusal",
        "response.regex.ssn",
        "response.regex.url",
        "response.sentiment",
        "response.sentiment.sentiment_score",
        "response.similarity",
        "response.similarity.CUSTOM_COLUMN",
        "response.similarity.context",
        "response.similarity.prompt",
        "response.similarity.refusal",
        "response.stats",
        "response.stats.char_count",
        "response.stats.difficult_words",
        "response.stats.flesch_reading_ease",
        "response.stats.grade",
        "response.stats.letter_count",
        "response.stats.lexicon_count",
        "response.stats.sentence_count",
        "response.stats.token_count",
        "response.text_stat",
        "response.topics",
        "response.toxicity",
        "response.toxicity.toxicity_score",
        "response.util.embeddings",
    ]
    # DOCSUB_END

    assert sorted(all_metrics) == sorted(list(get_metric_names().keys()))


def test_score_components_all():
    behavior = "block"
    sensitivity = "medium"

    metrics = [
        *CachedRulesets.prompt_cost(behavior, sensitivity).metrics,
        *CachedRulesets.prompt_util(behavior, sensitivity).metrics,
        *CachedRulesets.prompt_misuse(behavior, sensitivity, topics=("medical",)).metrics,
        *CachedRulesets.prompt_bad_actors(behavior, sensitivity).metrics,
        *CachedRulesets.prompt_truthfulness(behavior, sensitivity, rag_enabled=False, hallucinations_enabled=False).metrics,
        *CachedRulesets.prompt_customer_experience(behavior, sensitivity).metrics,
        *CachedRulesets.response_cost(behavior, sensitivity).metrics,
        *CachedRulesets.response_util(behavior, sensitivity).metrics,
        *CachedRulesets.response_misuse(behavior, sensitivity, topics=("medical",)).metrics,
        *CachedRulesets.response_bad_actors(behavior, sensitivity).metrics,
        *CachedRulesets.response_truthfulness(behavior, sensitivity, rag_enabled=False, hallucinations_enabled=False).metrics,
        *CachedRulesets.response_customer_experience(behavior, sensitivity).metrics,
    ]

    wf = Workflow(metrics=metrics)

    rulesets = [
        PromptCostRuleSet(ruleset="prompt.score.cost", options=CostRuleSetOptions(behavior=behavior, sensitivity=sensitivity)),
        ResponseCostRuleSet(ruleset="response.score.cost", options=CostRuleSetOptions(behavior=behavior, sensitivity=sensitivity)),
        PromptMisuseRuleSet(
            ruleset="prompt.score.misuse", options=MisuseRuleSetOptions(behavior=behavior, sensitivity=sensitivity, topics=("medical",))
        ),
        ResponseMisuseRuleSet(
            ruleset="response.score.misuse", options=MisuseRuleSetOptions(behavior=behavior, sensitivity=sensitivity, topics=("medical",))
        ),
        PromptBadActorsRuleSet(
            ruleset="prompt.score.bad_actors", options=BadActorsRuleSetOptions(behavior=behavior, sensitivity=sensitivity)
        ),
        ResponseBadActorsRuleSet(
            ruleset="response.score.bad_actors", options=BadActorsRuleSetOptions(behavior=behavior, sensitivity=sensitivity)
        ),
        PromptTruthfulnessRuleSet(
            ruleset="prompt.score.truthfulness",
            options=TruthfulnessRuleSetOptions(behavior=behavior, sensitivity=sensitivity, rag_enabled=False, hallucinations_enabled=False),
        ),
        ResponseTruthfulnessRuleSet(
            ruleset="response.score.truthfulness",
            options=TruthfulnessRuleSetOptions(behavior=behavior, sensitivity=sensitivity, rag_enabled=False, hallucinations_enabled=False),
        ),
        PromptCustomerExperienceRuleSet(
            ruleset="prompt.score.customer_experience", options=CustomerExperienceRuleSetOptions(behavior=behavior, sensitivity=sensitivity)
        ),
        ResponseCustomerExperienceRuleSet(
            ruleset="response.score.customer_experience",
            options=CustomerExperienceRuleSetOptions(behavior=behavior, sensitivity=sensitivity),
        ),
    ]

    score_wf = create_score_workflow(rulesets)

    result = score_wf.run(wf.run({"prompt": "prompt", "response": "response"}).metrics)

    columns = cast(List[str], result.metrics.columns)

    expected_columns = [
        "prompt.score.bad_actors",
        "prompt.score.bad_actors.prompt.similarity.injection",
        "prompt.score.bad_actors.prompt.similarity.injection_neighbor_coordinates",
        "prompt.score.bad_actors.prompt.similarity.injection_neighbor_ids",
        "prompt.score.cost",
        "prompt.score.cost.prompt.stats.char_count",
        "prompt.score.cost.prompt.stats.token_count",
        "prompt.score.customer_experience",
        "prompt.score.customer_experience.prompt.pii.credit_card",
        "prompt.score.customer_experience.prompt.pii.email_address",
        "prompt.score.customer_experience.prompt.pii.phone_number",
        "prompt.score.customer_experience.prompt.pii.redacted",
        "prompt.score.customer_experience.prompt.pii.us_bank_number",
        "prompt.score.customer_experience.prompt.pii.us_ssn",
        "prompt.score.customer_experience.prompt.sentiment.sentiment_score",
        "prompt.score.misuse",
        "prompt.score.misuse.prompt.topics.medical",
        "response.score.cost",
        "response.score.cost.response.stats.char_count",
        "response.score.cost.response.stats.token_count",
        "response.score.customer_experience",
        "response.score.customer_experience.response.regex.refusal",
        "response.score.customer_experience.response.sentiment.sentiment_score",
        "response.score.customer_experience.response.toxicity.toxicity_score",
        "response.score.misuse",
        "response.score.misuse.response.pii.credit_card",
        "response.score.misuse.response.pii.email_address",
        "response.score.misuse.response.pii.phone_number",
        "response.score.misuse.response.pii.redacted",
        "response.score.misuse.response.pii.us_bank_number",
        "response.score.misuse.response.pii.us_ssn",
        "response.score.truthfulness",
        "response.score.truthfulness.response.similarity.prompt",
    ]

    assert sorted(columns) == sorted(expected_columns + ["id"])


def test_score_components_misuse():
    behavior = "block"
    sensitivity = "medium"

    metrics = [
        *CachedRulesets.prompt_misuse(behavior, sensitivity, topics=("medical",)).metrics,
        *CachedRulesets.response_misuse(behavior, sensitivity, topics=("medical",)).metrics,
    ]

    wf = Workflow(metrics=metrics)

    rulesets = [
        PromptMisuseRuleSet(
            ruleset="prompt.score.misuse", options=MisuseRuleSetOptions(behavior=behavior, sensitivity=sensitivity, topics=("medical",))
        ),
        ResponseMisuseRuleSet(
            ruleset="response.score.misuse", options=MisuseRuleSetOptions(behavior=behavior, sensitivity=sensitivity, topics=("medical",))
        ),
    ]

    score_wf = create_score_workflow(rulesets)

    result = score_wf.run(wf.run({"prompt": "prompt", "response": "response"}).metrics)

    columns = cast(List[str], result.metrics.columns)

    # DOCSUB_START ruleset_misuse_metrics
    ruleset_misuse_metrics = [
        "prompt.score.misuse",
        # Where the only topic configured was 'medical' in this example
        "prompt.score.misuse.prompt.topics.medical",
        "response.score.misuse",
        "response.score.misuse.response.pii.credit_card",
        "response.score.misuse.response.pii.email_address",
        "response.score.misuse.response.pii.phone_number",
        "response.score.misuse.response.pii.redacted",
        "response.score.misuse.response.pii.us_bank_number",
        "response.score.misuse.response.pii.us_ssn",
    ]
    # DOCSUB_END

    assert sorted(columns) == sorted(ruleset_misuse_metrics + ["id"])


def test_score_components_bad_actor():
    behavior = "block"
    sensitivity = "medium"

    metrics = [
        *CachedRulesets.prompt_bad_actors(behavior, sensitivity).metrics,
        *CachedRulesets.response_bad_actors(behavior, sensitivity).metrics,
    ]

    wf = Workflow(metrics=metrics)

    rulesets = [
        PromptBadActorsRuleSet(
            ruleset="prompt.score.bad_actors", options=BadActorsRuleSetOptions(behavior=behavior, sensitivity=sensitivity)
        ),
        ResponseBadActorsRuleSet(
            ruleset="response.score.bad_actors", options=BadActorsRuleSetOptions(behavior=behavior, sensitivity=sensitivity)
        ),
    ]

    score_wf = create_score_workflow(rulesets)

    result = score_wf.run(wf.run({"prompt": "prompt", "response": "response"}).metrics)

    columns = cast(List[str], result.metrics.columns)

    # DOCSUB_START ruleset_bad_actor_metrics
    bad_actor_metrics = [
        "prompt.score.bad_actors",
        "prompt.score.bad_actors.prompt.similarity.injection",
        "prompt.score.bad_actors.prompt.similarity.injection_neighbor_coordinates",
        "prompt.score.bad_actors.prompt.similarity.injection_neighbor_ids",
    ]
    # DOCSUB_END

    assert sorted(columns) == sorted(bad_actor_metrics + ["id"])


def test_score_components_customer_experience():
    behavior = "block"
    sensitivity = "medium"

    metrics = [
        *CachedRulesets.prompt_customer_experience(behavior, sensitivity).metrics,
        *CachedRulesets.response_customer_experience(behavior, sensitivity).metrics,
    ]

    wf = Workflow(metrics=metrics)

    rulesets = [
        PromptCustomerExperienceRuleSet(
            ruleset="prompt.score.customer_experience", options=CustomerExperienceRuleSetOptions(behavior=behavior, sensitivity=sensitivity)
        ),
        ResponseCustomerExperienceRuleSet(
            ruleset="response.score.customer_experience",
            options=CustomerExperienceRuleSetOptions(behavior=behavior, sensitivity=sensitivity),
        ),
    ]

    score_wf = create_score_workflow(rulesets)

    result = score_wf.run(wf.run({"prompt": "prompt", "response": "response"}).metrics)

    columns = cast(List[str], result.metrics.columns)

    # DOCSUB_START ruleset_customer_experience_metrics
    customer_experience_metrics = [
        "prompt.score.customer_experience",
        "prompt.score.customer_experience.prompt.pii.credit_card",
        "prompt.score.customer_experience.prompt.pii.email_address",
        "prompt.score.customer_experience.prompt.pii.phone_number",
        "prompt.score.customer_experience.prompt.pii.redacted",
        "prompt.score.customer_experience.prompt.pii.us_bank_number",
        "prompt.score.customer_experience.prompt.pii.us_ssn",
        "prompt.score.customer_experience.prompt.sentiment.sentiment_score",
        "response.score.customer_experience",
        "response.score.customer_experience.response.regex.refusal",
        "response.score.customer_experience.response.sentiment.sentiment_score",
        "response.score.customer_experience.response.toxicity.toxicity_score",
    ]
    # DOCSUB_END

    assert sorted(columns) == sorted(customer_experience_metrics + ["id"])


def test_score_components_truthfulness():
    behavior = "block"
    sensitivity = "medium"

    metrics = [
        *CachedRulesets.prompt_truthfulness(behavior, sensitivity, rag_enabled=False, hallucinations_enabled=False).metrics,
        *CachedRulesets.response_truthfulness(behavior, sensitivity, rag_enabled=False, hallucinations_enabled=False).metrics,
    ]

    wf = Workflow(metrics=metrics)

    rulesets = [
        PromptTruthfulnessRuleSet(
            ruleset="prompt.score.truthfulness",
            options=TruthfulnessRuleSetOptions(behavior=behavior, sensitivity=sensitivity, rag_enabled=False, hallucinations_enabled=False),
        ),
        ResponseTruthfulnessRuleSet(
            ruleset="response.score.truthfulness",
            options=TruthfulnessRuleSetOptions(behavior=behavior, sensitivity=sensitivity, rag_enabled=False, hallucinations_enabled=False),
        ),
    ]

    score_wf = create_score_workflow(rulesets)

    result = score_wf.run(wf.run({"prompt": "prompt", "response": "response"}).metrics)

    columns = cast(List[str], result.metrics.columns)

    # DOCSUB_START ruleset_truthfulness_metrics
    truthfulness_metrics = [
        "response.score.truthfulness",
        "response.score.truthfulness.response.similarity.prompt",
    ]
    # DOCSUB_END

    assert sorted(columns) == sorted(truthfulness_metrics + ["id"])


def test_score_components_cost():
    behavior = "block"
    sensitivity = "medium"

    metrics = [
        *CachedRulesets.prompt_cost(behavior, sensitivity).metrics,
        *CachedRulesets.response_cost(behavior, sensitivity).metrics,
    ]

    wf = Workflow(metrics=metrics)

    rulesets = [
        PromptCostRuleSet(ruleset="prompt.score.cost", options=CostRuleSetOptions(behavior=behavior, sensitivity=sensitivity)),
        ResponseCostRuleSet(ruleset="response.score.cost", options=CostRuleSetOptions(behavior=behavior, sensitivity=sensitivity)),
    ]

    score_wf = create_score_workflow(rulesets)

    result = score_wf.run(wf.run({"prompt": "prompt", "response": "response"}).metrics)

    columns = cast(List[str], result.metrics.columns)

    # DOCSUB_START ruleset_cost_metrics
    cost_metrics = [
        "prompt.score.cost",
        "prompt.score.cost.prompt.stats.char_count",
        "prompt.score.cost.prompt.stats.token_count",
        "response.score.cost",
        "response.score.cost.response.stats.char_count",
        "response.score.cost.response.stats.token_count",
    ]
    # DOCSUB_END

    assert sorted(columns) == sorted(cost_metrics + ["id"])
