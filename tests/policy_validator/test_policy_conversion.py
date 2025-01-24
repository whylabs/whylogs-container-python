import pytest

from policy_validator.policy_conversion import convert_policy
from whylogs_container.whylabs.container.policy.policy_parsing import load_policy_from_str


def test_convert_010_to_001():
    yaml_010 = """
id: default_policy_id
policy_version: 1
schema_version: 0.1.0
whylabs_dataset_id: default

org_id: default

metrics:
  - metric: prompt.stats.token_count
  - metric: prompt.stats.char_count
  - metric: response.stats.token_count
  - metric: response.sentiment.sentiment_score

validators:
  - validator: constraint
    options:
      target_metric: response.stats.token_count
      upper_threshold: 10

  - validator: constraint
    options:
      target_metric: response.sentiment.sentiment_score
      upper_threshold: 0
    """
    policy = load_policy_from_str(yaml_010)

    converted = convert_policy(policy, "0.0.1", {})

    # remove the lines with DELETE on them
    expected_yaml = "\n".join(line for line in yaml_010.split("\n") if "org_id" not in line).replace("0.1.0", "0.0.1")

    expected_policy = load_policy_from_str(expected_yaml)

    assert expected_policy.model_dump() == converted.model_dump()


def test_convert_001_to_010():
    yaml_001 = """
id: default_policy_id
policy_version: 1
schema_version: 0.0.1
whylabs_dataset_id: default

metrics:
  - metric: prompt.stats.token_count
  - metric: prompt.stats.char_count
  - metric: response.stats.token_count
  - metric: response.sentiment.sentiment_score

validators:
  - validator: constraint
    options:
      target_metric: response.stats.token_count
      upper_threshold: 10

  - validator: constraint
    options:
      target_metric: response.sentiment.sentiment_score
      upper_threshold: 0
      """

    policy = load_policy_from_str(yaml_001)

    converted = convert_policy(policy, "0.1.0", {"org_id": "fish"})

    expected_yaml = """
id: default_policy_id
policy_version: 1
schema_version: 0.1.0
org_id: fish
whylabs_dataset_id: default

metrics:
  - metric: prompt.stats.token_count
  - metric: prompt.stats.char_count
  - metric: response.stats.token_count
  - metric: response.sentiment.sentiment_score

validators:
  - validator: constraint
    options:
      target_metric: response.stats.token_count
      upper_threshold: 10

  - validator: constraint
    options:
      target_metric: response.sentiment.sentiment_score
      upper_threshold: 0
      """

    expected_policy = load_policy_from_str(expected_yaml)

    assert expected_policy.model_dump() == converted.model_dump()


def test_failure_for_upgrading():
    yaml_001 = """
id: default_policy_id
policy_version: 1
schema_version: 0.0.1
whylabs_dataset_id: default

metrics:
  - metric: prompt.stats.token_count
  - metric: prompt.stats.char_count
  - metric: response.stats.token_count
  - metric: response.sentiment.sentiment_score

validators:
  - validator: constraint
    options:
      target_metric: response.stats.token_count
      upper_threshold: 10

  - validator: constraint
    options:
      target_metric: response.sentiment.sentiment_score
      upper_threshold: 0
      """

    policy = load_policy_from_str(yaml_001)

    # will fail here because we didn't supply a value to use for the org_id, which is required in 0.1.0
    with pytest.raises(ValueError):
        convert_policy(policy, "0.1.0", {})
