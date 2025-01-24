import pandas as pd

from langkit.core.metric import SingleMetricResult
from langkit.core.workflow import MetricCreator, SingleMetric, Workflow
from whylogs_container.whylabs.container.otel import extract_score_causes
from whylogs_container.whylabs.container.policy.policy_0_0_1 import (
    CustomerExperienceRuleSetOptions,
    MisuseRuleSetOptions,
    PromptMisuseRuleSet,
    ResponseCustomerExperienceRuleSet,
)
from whylogs_container.whylabs.container.policy.scores import create_score_workflow


def prompt_topic_financial() -> MetricCreator:
    def udf(text: pd.DataFrame) -> SingleMetricResult:
        # hardcode it to always return a high score
        return SingleMetricResult(metrics=[0.9 for _ in range(len(text))])

    return lambda: SingleMetric(name="prompt.topics.financial", input_names=["prompt"], evaluate=udf)


def prompt_topic_medical() -> MetricCreator:
    def udf(text: pd.DataFrame) -> SingleMetricResult:
        return SingleMetricResult(metrics=[0.1 for _ in range(len(text))])

    return lambda: SingleMetric(name="prompt.topics.medical", input_names=["prompt"], evaluate=udf)


def response_refusal() -> MetricCreator:
    def udf(text: pd.DataFrame) -> SingleMetricResult:
        return SingleMetricResult(metrics=[1 for _ in range(len(text))])

    return lambda: SingleMetric(name="response.regex.refusal", input_names=["response"], evaluate=udf)


def response_toxicity() -> MetricCreator:
    def udf(text: pd.DataFrame) -> SingleMetricResult:
        return SingleMetricResult(metrics=[0.2 for _ in range(len(text))])

    return lambda: SingleMetric(name="response.toxicity.toxicity_score", input_names=["response"], evaluate=udf)


def test_extract_tags_failures():
    behavior = "block"
    sensitivity = "medium"
    topics = ("financial", "medical")

    wf = Workflow(metrics=[prompt_topic_financial, prompt_topic_medical, response_refusal, response_toxicity])
    rulesets = [
        PromptMisuseRuleSet(
            ruleset="prompt.score.misuse", options=MisuseRuleSetOptions(behavior=behavior, sensitivity=sensitivity, topics=topics)
        ),
        ResponseCustomerExperienceRuleSet(
            ruleset="response.score.customer_experience",
            options=CustomerExperienceRuleSetOptions(behavior=behavior, sensitivity=sensitivity),
        ),
    ]
    score_wf = create_score_workflow(rulesets)

    # content of the promt doesn't matter, metrics are fake
    result = wf.run({"prompt": "..", "response": ".."})
    score_result = score_wf.run(result.metrics)

    assert sorted(extract_score_causes(score_result)) == sorted(
        [
            "prompt.score.misuse.prompt.topics.financial",
            "response.score.customer_experience.response.regex.refusal",
        ]
    )


def test_extract_tags_no_failures():
    # Omit the failure metrics for this test. There should be nothing returned as score causes
    # because there are no failures.
    behavior = "block"
    sensitivity = "medium"
    topics = ("financial", "medical")

    wf = Workflow(metrics=[prompt_topic_medical, response_toxicity])
    rulesets = [
        PromptMisuseRuleSet(
            ruleset="prompt.score.misuse", options=MisuseRuleSetOptions(behavior=behavior, sensitivity=sensitivity, topics=topics)
        ),
        ResponseCustomerExperienceRuleSet(
            ruleset="response.score.customer_experience",
            options=CustomerExperienceRuleSetOptions(behavior=behavior, sensitivity=sensitivity),
        ),
    ]
    score_wf = create_score_workflow(rulesets)

    # content of the promt doesn't matter, metrics are fake
    result = wf.run({"prompt": "..", "response": ".."})
    score_result = score_wf.run(result.metrics)

    assert extract_score_causes(score_result) == []
