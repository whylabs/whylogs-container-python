import logging

import pandas as pd

from langkit.core.workflow import Workflow
from whylogs_container.whylabs.container.policy.policy_0_0_1 import (
    BadActorsRuleSetOptions,
    CustomerExperienceRuleSetOptions,
    Policy,
    ResponseBadActorsRuleSet,
    ResponseCustomerExperienceRuleSet,
)
from whylogs_container.whylabs.container.policy.scores import create_score_workflow

TEST_LOGGER = logging.getLogger()


def test_ruleset_udf() -> None:
    # customer experience test
    df = pd.DataFrame({"prompt": ["Hello there"], "response": ["Hello there"]})

    ruleset1 = ResponseBadActorsRuleSet(
        ruleset="response.score.bad_actors", options=BadActorsRuleSetOptions(behavior="block", sensitivity="medium")
    )
    ruleset2 = ResponseCustomerExperienceRuleSet(
        ruleset="response.score.customer_experience", options=CustomerExperienceRuleSetOptions(behavior="block", sensitivity="medium")
    )
    policy = Policy(rulesets=(ruleset1, ruleset2), whylabs_dataset_id="test", id="test", policy_version=1)
    policy_options = policy.to_langkit_options()

    metric_wf = Workflow(
        metrics=policy_options.metrics,
        validators=policy_options.validators,
        callbacks=policy_options.callbacks,
        cache_assets=False,
    )

    metric_result = metric_wf.run(df)

    wf = create_score_workflow([ruleset1, ruleset2])
    result = wf.run(metric_result.metrics)

    expected = [
        {
            "id": "0",
            "response.score.customer_experience": 34,
            "response.score.customer_experience.response.regex.refusal": 1,
            "response.score.customer_experience.response.sentiment.sentiment_score": 34,
            "response.score.customer_experience.response.toxicity.toxicity_score": 1,
        },
    ]

    assert expected == result.metrics.to_dict(orient="records")  # pyright: ignore[reportUnknownMemberType]
