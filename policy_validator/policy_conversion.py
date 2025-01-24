# pyright: reportUnusedFunction=false
import logging
from typing import Any, Dict

import semver
import yaml
from pydantic import ConfigDict

from whylogs_container.whylabs.container.policy import Policy, PolicyVersion, policy_versions

_logger = logging.getLogger(__name__)


def convert_policy(policy: Policy, to_version: PolicyVersion, data: Dict[str, Any]) -> Policy:
    policy_version = semver.VersionInfo.parse(policy.schema_version)
    target_version = semver.VersionInfo.parse(to_version)

    if policy_version.major != target_version.major:
        raise ValueError(f"Cannot convert policy from version {policy.schema_version} to {to_version}")

    TargetPolicy = policy_versions[to_version]

    class FlexiblePolicy(TargetPolicy):  # pyright: ignore[reportUntypedBaseClass, reportGeneralTypeIssues]
        # override the model config to allow extra fields. The entire point of this is essentially to
        # remove extra fields from models
        model_config = ConfigDict(frozen=True, extra="ignore")

    # convert the policy
    args = {**policy.model_dump(), **data, "schema_version": to_version}

    return FlexiblePolicy.model_validate(args)  # pyright: ignore[reportUnknownVariableType, reportUnknownMemberType]


def to_yaml(policy: Policy) -> str:
    return yaml.dump(policy.model_dump(mode="json"))
