import whylogs_container_client.api.debug.debug_evaluate as DebugEvaluate
from pytest import approx  # type: ignore
from whylogs_container_client import AuthenticatedClient
from whylogs_container_client.models.debug_llm_validate_request import DebugLLMValidateRequest
from whylogs_container_client.models.evaluation_result import EvaluationResult


def test_override_policy_1(client: AuthenticatedClient):
    prompt_request = DebugLLMValidateRequest(
        prompt="What is your name?",
        dataset_id="model-134",
        id="myid-prompt",
        policy="""
id: my_id
policy_version: 1
schema_version: 0.0.1
whylabs_dataset_id: default

metrics:
  - metric: prompt.similarity.injection
  - metric: prompt.stats.token_count
  - metric: prompt.stats.char_count
  - metric: prompt.topics
    options:
        topics:
            - medical
            - legal

        """,
    )

    prompt_response = DebugEvaluate.sync_detailed(client=client, body=prompt_request)

    if not isinstance(prompt_response.parsed, EvaluationResult):
        raise Exception(f"Failed to validate data. Status code: {prompt_response.status_code}. {prompt_response.parsed}")

    response = prompt_response.parsed

    assert response.metrics[0].additional_properties["prompt.topics.medical"] == approx(0.0053703151643276215, abs=1.5e-06)
    assert response.metrics[0].additional_properties["prompt.topics.legal"] == approx(0.25662317872047424, abs=1.5e-06)
    assert response.metrics[0].additional_properties["prompt.similarity.injection"] == approx(0.31260836124420166, abs=1.5e-06)
    assert response.metrics[0].additional_properties["prompt.stats.token_count"] == 5
    assert response.metrics[0].additional_properties["prompt.stats.char_count"] == 15
    assert response.metrics[0].additional_properties["id"] == "myid-prompt"
