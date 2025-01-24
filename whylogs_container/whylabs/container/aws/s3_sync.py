# pyright: reportUnknownVariableType=false, reportUnknownMemberType=false, reportUnknownArgumentType=false
# Disable types here sadly because the boto3 client is not typed at all
import logging
from typing import Any, Generator, List, Mapping

from langkit.core.workflow import Workflow
from whylogs_container.whylabs.container.aws.client import get_s3_client
from whylogs_container.whylabs.container.policy import Policy
from whylogs_container.whylabs.container.policy.policy_parsing import load_policy_from_str
from whylogs_container.whylabs.container.server_config import get_server_config
from whylogs_container_types import DatasetId

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class S3Sync:
    s3_llm_validators: Mapping[DatasetId, Workflow]

    def __init__(self, region_name: str | None = None):
        self.env_vars = get_server_config()

        if not self.env_vars.s3_config_bucket_name:
            raise ValueError("S3 bucket name is not defined. Please set S3_CONFIG_BUCKET_NAME as an environment variable.")

        self.s3_client = get_s3_client()

    def _list_s3_yaml_objects(self) -> Generator[Any, Any, None]:
        paginator = self.s3_client.get_paginator(operation_name="list_objects_v2")
        for page in paginator.paginate(Bucket=self.env_vars.s3_config_bucket_name, Prefix=self.env_vars.s3_config_prefix):
            for obj in page.get("Contents", []):
                if obj.get("Key", "").endswith((".yaml", ".yml")):
                    yield obj

    def sync_s3_policies(self) -> List[Policy]:
        logger.info("Syncing S3 to container schemas.")
        s3_keys = [obj.get("Key") for obj in self._list_s3_yaml_objects() if obj.get("Key")]

        s3_policies: List[Policy] = []

        for s3_key in s3_keys:
            # Filter out the Nones because the types make you, but this really can't happen. Key is the s3 file path.
            if s3_key is None:
                logger.warning("Found None values on the s3 list, skipping.")
                continue
            s3_response = self.s3_client.get_object(Bucket=self.env_vars.s3_config_bucket_name, Key=s3_key)
            file_content = s3_response["Body"].read().decode("utf-8")
            policy = load_policy_from_str(file_content)
            # We are already having to parse the file into a Policy object, so it doesn't make sense to only
            # update based on the policy_version. Everything that lives on S3 will be synced to the container.
            # If they remove it from s3, it will be removed from the container schemas as well.
            s3_policies.append(policy)

        return s3_policies
