from typing import Dict, List, Optional, Union

from pydantic import BaseModel, Field

DataTypes = Union[str, int, float, bool, List[float], List[int], List[str]]


class LogMultiple(BaseModel):
    columns: List[str]
    data: List[List[DataTypes]]


class LogRequest(BaseModel):
    dataset_id: str = Field(None, alias="datasetId")
    timestamp: Optional[int]
    single: Optional[Dict[str, DataTypes]]
    multiple: Optional[LogMultiple]
