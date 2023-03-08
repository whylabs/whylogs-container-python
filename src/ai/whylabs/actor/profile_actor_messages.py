from dataclasses import dataclass
from typing import Dict, List, Optional, TypedDict

import orjson

from ..container.requests import DataTypes


class MultipleDict(TypedDict):
    columns: List[str]
    data: List[List[DataTypes]]


class LogRequestDict(TypedDict):
    datasetId: str
    timestamp: int
    single: Optional[Dict[str, DataTypes]]
    multiple: Optional[MultipleDict]


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

        if ("single" not in d or d["single"] is None) and ("multiple" not in d or d["multiple"] is None):
            raise Exception(f"Request has neither single nor multiple field {d}")

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
