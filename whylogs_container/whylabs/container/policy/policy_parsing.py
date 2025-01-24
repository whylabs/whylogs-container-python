import logging
import urllib.parse
from pprint import pformat
from typing import Dict, List, Set, Tuple

import yaml
from pydantic import ValidationError
from pydantic_core import ErrorDetails

from whylogs_container.whylabs.container.policy import Policy, policy_versions
from whylogs_container.whylabs.container.version import version

logger = logging.getLogger(__name__)


def _redact_sensitive_fields(yaml_str: str) -> str:
    # defensively replace any line that contains the key words with ***
    keywords = ["auth", "key", "secret", "token", "password"]
    for keyword in keywords:
        yaml_str = "\n".join("# redacted" if keyword in line.lower() else line for line in yaml_str.split("\n"))
    return yaml_str


_policy_editor_url = "https://secure-policy-editor.whylabsapp.com"


def load_policy_from_str(yaml_str: str) -> Policy:
    data = yaml.safe_load(yaml_str)
    logger.debug("Loaded policy: %s", pformat(data, indent=2, width=200))
    schema_version_field = data.get("schema_version")

    if not schema_version_field:
        raise Exception("Policy schema version is missing")

    schema_version = str(schema_version_field)
    Policy = policy_versions.get(schema_version)

    if not Policy:
        raise Exception(f"Policy schema version {schema_version} is not supported")

    try:
        return Policy(**data)
    except ValidationError as e:
        redacted = _redact_sensitive_fields(yaml_str)
        content = urllib.parse.quote(redacted)
        url = f"{_policy_editor_url}/{version}/index.html?content={content}"
        raise Exception(f"""Failed to load policy with content:

{redacted}

Visualize the errors at {url}

Failure Report:

{generate_validation_report(e)}
""")


def generate_validation_report(error: ValidationError) -> str:
    errors: List[ErrorDetails] = error.errors()
    error_summary: Dict[str, Set[str]] = {}
    seen_errors: Set[Tuple[str, str, str]] = set()

    for err in errors:
        err_type = err.get("type", "unknown")
        input_value = str(err.get("input", "<missing>"))
        loc_list = err.get("loc", [])

        # Determine the highest-level location (e.g., 'rulesets[0]')
        if len(loc_list) >= 2 and isinstance(loc_list[1], int):
            highest_loc = f"{loc_list[0]}[{loc_list[1]}]"
        elif len(loc_list) >= 1:
            highest_loc = str(loc_list[0])
        else:
            highest_loc = "<unknown location>"

        # Create a unique key for deduplication
        error_key: Tuple[str, str, str] = (err_type, input_value, highest_loc)
        if error_key in seen_errors:
            continue  # Skip duplicate errors
        seen_errors.add(error_key)

        field = loc_list[-1] if loc_list else "<unknown>"
        # Construct error messages based on error type
        if err_type == "literal_error":
            message = f"Invalid value '{input_value}' in field '{field}' at '{highest_loc}'"
        elif err_type == "missing":
            message = f"Missing required field(s) in '{field}' at '{highest_loc}'"
        elif err_type == "extra_forbidden":
            message = f"Extra field not allowed in '{field}' at '{highest_loc}'"
        else:
            message = f"Validation error at '{highest_loc}': {err.get('msg', 'Unknown error')}"

        error_summary.setdefault(err_type, set()).add(message)

    # Build the final report
    report_lines: List[str] = []
    for err_type, messages in error_summary.items():
        report_lines.append(f"{err_type}:")
        for msg in sorted(messages):
            report_lines.append(f"  - {msg}")
    return "\n".join(report_lines)
