import logging
import time
from enum import Enum
from typing import Dict, List, Optional, Sequence, Union

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, field_validator

from langkit.core.workflow import InputContext, RunOptions
from whylogs.api.logger.experimental.logger.actor.process_rolling_logger import DataDict
from whylogs.api.logger.experimental.logger.actor.process_rolling_logger_messages import LogEmbeddingRequestDict, LogRequestDict
from whylogs_container.whylabs.container.dependencies.api_key import WhyLabsApiKey

EmbeddingDataTypes = Union[List[List[float]], List[List[int]], List[List[str]]]
DataTypes = Union[str, int, float, bool, List[float], List[int], List[str], None, EmbeddingDataTypes]

_logger = logging.getLogger(__name__)


class LogMultiple(BaseModel):
    columns: Sequence[str]
    data: Sequence[Sequence[DataTypes]]


class LogRequest(BaseModel):
    dataset_id: str = Field(alias="datasetId")
    multiple: LogMultiple
    timestamp: Optional[int] = None

    def to_log_request_dict(self, api_key: WhyLabsApiKey) -> LogRequestDict:
        return LogRequestDict(
            datasetId=self.dataset_id,
            timestamp=self.timestamp,
            multiple=DataDict(columns=self.multiple.columns, data=self.multiple.data),  # pyright: ignore[reportArgumentType]
            orgId=api_key.org_id,
        )


class LogEmbeddingRequest(BaseModel):
    datasetId: str = Field(alias="dataset_id")
    timestamp: int
    embeddings: Dict[str, EmbeddingDataTypes]

    # LogEmbeddingRequestDict
    def to_log_embedding_request_dict(self, api_key: WhyLabsApiKey) -> LogEmbeddingRequestDict:
        return LogEmbeddingRequestDict(
            datasetId=self.datasetId,
            timestamp=self.timestamp,
            embeddings=self.embeddings,  # pyright: ignore[reportArgumentType]
            orgId=api_key.org_id,
        )


class PubSubMessage(BaseModel):
    attributes: Dict[str, str]
    data: str
    messageId: str = Field(alias="message_id")
    publishTime: str = Field(alias="publish_time")


class PubSubRequest(BaseModel):
    subscription: str
    message: PubSubMessage


AdditionalDataDict = Dict[str, Union[str, int, float]]

# Somewhat arbitrary cutoff for early timestamp limits (2000-01-01). Meant to combat mistakenly
# provided timestamps that are too early.
_EARLIEST_ALLOWED_TIMESTAMP = 946688400000


class LLMValidateRequest(BaseModel):
    model_config = ConfigDict(frozen=True, populate_by_name=True)

    prompt: Optional[str] = None
    response: Optional[str] = None
    context: Optional[InputContext] = None
    id: Optional[str] = None
    dataset_id: str = Field(alias="datasetId")
    timestamp: int = Field(default_factory=lambda: int(time.time() * 1000))
    additional_data: AdditionalDataDict = Field(default_factory=lambda: {})
    options: Optional[RunOptions] = None
    metadata: Optional[Dict[str, str]] = None

    @field_validator("timestamp")
    @classmethod
    def replace_old_times(cls, v: int) -> int:
        # If the provided timestamp is super old (epoch start-ish) then just use the default
        # instead anyway
        if v < _EARLIEST_ALLOWED_TIMESTAMP:
            _logger.warning("Provided timestamp is too old (%s), using current time instead", v)
            return int(time.time() * 1000)
        return v

    def to_row(self) -> Dict[str, Union[str, int, float, InputContext]]:
        if not self.prompt and not self.response:
            raise ValueError("Must supply at least a prompt or response.")

        row: Dict[str, Union[str, int, float, InputContext]] = {}

        if self.prompt:
            row["prompt"] = self.prompt

        if self.response:
            row["response"] = self.response

        if self.id:
            row["id"] = self.id

        if self.context:
            row["context"] = self.context

        if self.additional_data:
            for k, v in self.additional_data.items():
                row[k] = v

        return row

    def to_dataframe(self) -> pd.DataFrame:
        return pd.DataFrame([self.to_row()])

    def to_data_dict(self) -> DataDict:
        if not self.prompt and not self.response:
            raise ValueError("Must supply at least a prompt or response.")

        dd = DataDict(columns=[], data=[[]])

        if self.additional_data:
            for k, v in self.additional_data.items():
                dd["columns"].append(k)
                dd["data"][0].append(v)

        return dd


class EmbeddingRequest(BaseModel):
    model_config = ConfigDict(frozen=True, populate_by_name=True)

    prompt: Optional[str] = None
    response: Optional[str] = None
    context: Optional[InputContext] = None


class DebugLLMValidateRequest(LLMValidateRequest):
    policy: str


class Endpoints(Enum):
    Evaluate = "/evaluate"
    DebugEvaluate = "/debug/evaluate"
    LogLLM = "/log/llm"
