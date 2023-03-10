from typing import List, Optional, Union, Dict

from pydantic import BaseModel, Field

DataTypes = Union[str, int, float, bool, List[float], List[int], List[str]]


class LogMultiple(BaseModel):
    columns: List[str]
    data: List[List[DataTypes]]


class LogRequest(BaseModel):
    datasetId: str = Field(None, alias="dataset_id")
    timestamp: Optional[int]
    multiple: LogMultiple


class PubSubMessage(BaseModel):
    attributes: Dict[str, str]
    data: str
    messageId: str = Field(None, alias="message_id")
    publishTime: str = Field(None, alias="publish_time")


class PubSubRequest(BaseModel):
    subscription: str
    message: PubSubMessage
