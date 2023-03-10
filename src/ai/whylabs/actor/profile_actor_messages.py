from dataclasses import dataclass
import base64
from typing import Dict, List, TypedDict, Union

import numpy as np
import orjson
import pandas as pd
from whylabs_toolkit.container.config_types import DatasetCadence

from ...util.time import TimeGranularity, truncate_time_ms
from ..container.requests import DataTypes


class DataDict(TypedDict):
    columns: List[str]
    data: List[List[DataTypes]]


class LogRequestDict(TypedDict):
    datasetId: str
    timestamp: int
    multiple: DataDict


class LogEmbeddingRequestDict(TypedDict):
    datasetId: str
    timestamp: int
    embeddings: Dict[str, List[DataTypes]]


class PubSubMessage(TypedDict):
    attributes: Dict[str, str]
    data: str
    message_id: str
    publish_time: str  # TODO need a sample of the format


class PubSubDict(TypedDict):
    subscription: str
    message: PubSubMessage
    log_request: LogRequestDict


class DebugMessage:
    pass


class PublishMessage:
    pass


@dataclass
class RawLogMessage:
    request: bytes
    request_time: int

    def to_log_request_dict(self) -> LogRequestDict:
        d: LogRequestDict = orjson.loads(self.request)
        if "timestamp" not in d or d["timestamp"] is None:
            d["timestamp"] = self.request_time

        if "datasetId" not in d or d["datasetId"] is None:
            raise Exception(f"Request missing dataset id {d}")

        if "multiple" not in d or d["multiple"] is None:
            raise Exception(f"Request has no data field {d}")

        return d


def get_columns(request: LogRequestDict) -> List[str]:
    if "multiple" in request and request["multiple"] is not None:
        return request["multiple"]["columns"]

    raise Exception(f"Missing both single and data fields in request {request}.")


def get_embeddings_columns(request: LogEmbeddingRequestDict) -> List[str]:
    embeddings = request["embeddings"]
    if embeddings is not None:
        return list(embeddings.keys())

    raise Exception(f"Missing embeddings fields in request {request}.")


@dataclass
class RawPubSubMessage:
    request: bytes
    request_time: int

    # TODO unit tests
    def to_pubsub_message(self) -> PubSubDict:
        d: PubSubDict = orjson.loads(self.request)

        if "message" not in d or d["message"] is None:
            raise Exception(f"Request missing message field {d}")

        message = d["message"]
        encoded_data = message["data"]
        log_request_dict_bytes = base64.b64decode(encoded_data)

        # TODO verify desired behavior here. We could use the time inside of the pubsub message as well.
        log_message = RawLogMessage(request=log_request_dict_bytes, request_time=self.request_time)
        log_request = log_message.to_log_request_dict()
        d["log_request"] = log_request
        return d


@dataclass
class RawLogEmbeddingsMessage:
    request: bytes
    request_time: int

    def to_log_embeddings_request_dict(self) -> LogEmbeddingRequestDict:
        d: LogEmbeddingRequestDict = orjson.loads(self.request)
        if "timestamp" not in d or d["timestamp"] is None:
            d["timestamp"] = self.request_time

        if "datasetId" not in d or d["datasetId"] is None:
            raise Exception(f"Request missing dataset id {d}")

        if "embeddings" not in d or d["embeddings"] is None:
            raise Exception(f"Request has no embeddings field {d}")

        if not isinstance(d["embeddings"], dict):
            # TODO test recovering from errors like this. It seems to brick the container
            raise Exception(
                f'Expected a dictionary format for embeddings of the form {{"column_name": "embedding_2d_list"}}. Got {self.request}'
            )

        return d


def log_dict_to_data_frame(request: LogRequestDict) -> pd.DataFrame:
    return pd.DataFrame(request["multiple"]["data"], columns=request["multiple"]["columns"])


def log_dict_to_embedding_matrix(request: LogEmbeddingRequestDict) -> Dict[str, np.ndarray]:
    row: Dict[str, np.ndarray] = {}
    for col, embeddings in request["embeddings"].items():
        row[col] = np.array(embeddings)
    return row


def reduce_log_requests(acc: LogRequestDict, cur: LogRequestDict) -> LogRequestDict:
    """
    Reduce requests, assuming that each request has the same columns.
    That assumption should be enforced before this is used by grouping by set of columns.
    """
    acc["multiple"]["data"].extend(cur["multiple"]["data"])
    return acc


def reduce_embeddings_request(acc: LogEmbeddingRequestDict, cur: LogEmbeddingRequestDict) -> LogEmbeddingRequestDict:
    for col, embeddings in cur["embeddings"].items():
        if col not in acc["embeddings"]:
            acc["embeddings"][col] = []

        acc["embeddings"][col].extend(embeddings)

    return acc


def determine_dataset_timestamp(
    cadence: DatasetCadence, request: Union[LogRequestDict, LogEmbeddingRequestDict]
) -> int:
    ts = request["timestamp"]
    truncate_cadence = TimeGranularity.D if cadence == DatasetCadence.DAILY else TimeGranularity.H
    return truncate_time_ms(ts, truncate_cadence)
