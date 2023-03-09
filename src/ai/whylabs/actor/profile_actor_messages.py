from dataclasses import dataclass
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
    data: DataDict


class LogEmbeddingRequestDict(TypedDict):
    datasetId: str
    timestamp: int
    embeddings: Dict[str, List[DataTypes]]


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

        if "data" not in d or d["data"] is None:
            raise Exception(f"Request has no data field {d}")

        return d


def get_columns(request: LogRequestDict) -> List[str]:
    if "data" in request and request["data"] is not None:
        return request["data"]["columns"]

    raise Exception(f"Missing both single and data fields in request {request}.")


def get_embeddings_columns(request: LogEmbeddingRequestDict) -> List[str]:
    embeddings = request["embeddings"]
    if embeddings is not None:
        return list(embeddings.keys())

    raise Exception(f"Missing embeddings fields in request {request}.")


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
    return pd.DataFrame(request["data"]["data"], columns=request["data"]["columns"])


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
    acc["data"]["data"].extend(cur["data"]["data"])
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
    return truncate_time_ms(ts, TimeGranularity.D if cadence == DatasetCadence.DAILY else TimeGranularity.H)
