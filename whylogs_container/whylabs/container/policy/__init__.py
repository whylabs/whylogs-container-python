from typing import Annotated, Any, Dict, List, Literal, Union

from pydantic import Field, TypeAdapter

from whylogs_container.whylabs.container.policy.policy_0_0_1 import Policy as Policy_0_0_1
from whylogs_container.whylabs.container.policy.policy_0_0_1 import Rulesets as Rulesets_0_0_1
from whylogs_container.whylabs.container.policy.policy_0_1_0 import Policy as Policy_0_1_0
from whylogs_container.whylabs.container.policy.policy_0_1_0 import Rulesets as Rulesets_0_1_0
from whylogs_container.whylabs.container.server_config import get_server_config

Policy = Union[Policy_0_0_1, Policy_0_1_0]
Rulesets = Union[Rulesets_0_0_1, Rulesets_0_1_0]

PolicyVersion = Literal["0.0.1", "0.1.0"]

policy_versions = {
    "0.0.1": Policy_0_0_1,
    "0.1.0": Policy_0_1_0,
}

latest_policy_version = list(reversed(sorted(policy_versions.keys())))[0]


def get_union_type_adapter() -> TypeAdapter[Any]:
    return TypeAdapter(Annotated[Policy, Field(discriminator="schema_version")])


def get_union_schema() -> Dict[str, Any]:
    """
    Return a single json schema that represents a union of all of the policy version schemas
    """
    adapter = get_union_type_adapter()
    return adapter.json_schema()


def get_policy_org_id(policy: Policy) -> str:
    if policy.schema_version == "0.0.1":
        # This version of the schema doesn't contain org information so we default to using the org from the api key.
        # All policies will have an org id as part of the key internally even for the older formats that don't contain
        # one explicitly, and all policies will be looked up by a combination of model-id and org-id
        config = get_server_config()

        if config.tenancy_mode == "MULTI":
            raise ValueError(
                "Policy schema version 0.0.1 is not supported in multi-tenancy mode. This policy needs to be updated to "
                "include an org_id field at the top level \n\n"
                f"{policy}"
            )

        return config.org_id
    elif policy.schema_version == "0.1.0":
        return policy.org_id


__all__: List[str] = [
    "Policy",
    "policy_versions",
]

if __name__ == "__main__":
    import json

    print(json.dumps(get_union_schema()))  # ban: allow
