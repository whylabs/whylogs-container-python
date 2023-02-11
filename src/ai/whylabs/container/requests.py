from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Type, Union, cast

from pydantic import BaseModel, Field

DataTypes = Union[str, int, float, bool]


class LogMultiple(BaseModel):
    columns: List[str]
    data: List[List[DataTypes]]


class LogRequest(BaseModel):
    dataset_id: str = Field(None, alias="datasetId")
    # timestamp: Optional[int] # TODO enable in a follow up. Forces me to do a lot of date math and grouping for bulk message handling
    single: Optional[Dict[str, DataTypes]]
    multiple: Optional[LogMultiple]
