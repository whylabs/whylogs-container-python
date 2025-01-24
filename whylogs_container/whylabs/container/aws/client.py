# pyright: reportUnknownVariableType=false, reportUnknownMemberType=false, reportUnknownArgumentType=false
# Disable types here sadly because the boto3 client is not typed at all
import logging
from dataclasses import dataclass
from typing import Any

import boto3

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@dataclass
class AWSCredentials:
    aws_access_key_id: str
    aws_secret_access_key: str
    aws_session_token: str


# Wrapper to make Any not required, it messes with type checking in a few niche cases
@dataclass
class AWSClientWrapper:
    client: Any


def get_s3_client() -> Any:
    return boto3.client("s3")


def get_sqs_consumer_client() -> AWSClientWrapper:
    return AWSClientWrapper(boto3.client("sqs"))


def get_sqs_sender_client() -> AWSClientWrapper:
    return AWSClientWrapper(boto3.client("sqs"))
