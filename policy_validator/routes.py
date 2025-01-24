# pyright: reportUnusedFunction=false
import logging
from typing import Any, Dict, Optional

from fastapi import Body, FastAPI, HTTPException
from pydantic import BaseModel, Field

from policy_validator.policy_conversion import convert_policy, to_yaml
from whylogs_container.whylabs.container.policy import Policy, PolicyVersion, get_union_schema, policy_versions
from whylogs_container.whylabs.container.policy.policy_parsing import load_policy_from_str

_logger = logging.getLogger(__name__)


class ValidateRequest(BaseModel):
    yaml: str
    data: Dict[str, Any] = Field(default_factory=dict)


class ValidationResponse(BaseModel):
    valid: bool
    version: str


def init_routes():
    app = FastAPI()

    @app.get("/v1/policy/json-schema")
    def get_policy_schema(schema_version: Optional[PolicyVersion] = None):
        if schema_version:
            policy = policy_versions.get(schema_version)

            if policy is None:
                raise HTTPException(
                    status_code=404, detail="Policy schema version not found. Supported versions: " + ", ".join(policy_versions.keys())
                )

            return policy.model_json_schema()
        else:
            return get_union_schema()

    @app.post("/v1/policy/validate", tags=["validation"])
    def validate_policy(policy_yaml: str = Body(..., media_type="text/yaml")) -> ValidationResponse:
        try:
            policy = load_policy_from_str(policy_yaml)
            return ValidationResponse(valid=True, version=policy.schema_version)
        except Exception as e:
            _logger.exception("Policy validation failed", e)
            raise HTTPException(status_code=400, detail=f"Policy validation failed: {str(e)}")

    @app.post("/v1/policy/convert/json", tags=["conversion"])
    def _policy_convert_json(to_version: PolicyVersion, request: ValidateRequest) -> Policy:
        try:
            policy = load_policy_from_str(request.yaml)
            return convert_policy(policy, to_version, request.data)
        except Exception as e:
            _logger.exception("Policy validation failed", e)
            raise HTTPException(status_code=400, detail=f"Policy conversion failed: {str(e)}")

    @app.post("/v1/policy/convert", tags=["conversion"])
    def _policy_convert_yaml(to_version: PolicyVersion, policy: ValidateRequest) -> str:
        try:
            return to_yaml(_policy_convert_json(to_version, policy))
        except Exception as e:
            _logger.exception("Policy validation failed", e)
            raise HTTPException(status_code=400, detail=f"Policy conversion failed: {str(e)}")

    @app.get("/health", tags=["manage"])
    def _health() -> str:
        policy = """
id: builtin-default
policy_version: 1
schema_version: 0.0.1
whylabs_dataset_id: default

rulesets:
  - ruleset: score.misuse
    options:
      behavior: flag
      sensitivity: medium
      topics:
        - medical
        - legal
        - financial

  - ruleset: score.bad_actors
    options:
      behavior: block
      sensitivity: medium

  - ruleset: score.truthfulness
    options:
      behavior: flag
      sensitivity: medium
      rag_enabled: true
      hallucinations_enabled: false

  - ruleset: score.customer_experience
    options:
      behavior: flag
      sensitivity: medium

  - ruleset: score.cost
    options:
      behavior: flag
      sensitivity: medium
        """
        try:
            to_yaml(_policy_convert_json("0.0.1", ValidateRequest(yaml=policy)))
            return "OK"
        except Exception as e:
            _logger.exception("Policy validation failed", e)
            raise HTTPException(status_code=400, detail=f"Policy conversion failed: {str(e)}")

    return app
