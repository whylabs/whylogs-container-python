from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Union

from whylogs.core.schema import DatasetSchema


class DatasetCadence(Enum):
    HOURLY = "HOURLY"
    DAILY = "DAILY"


class DatasetUploadCadenceGranularity(Enum):
    MINUTE = "M"
    HOUR = "H"
    DAY = "D"


@dataclass(frozen=True)
class DatasetUploadCadence:
    interval: int
    granularity: DatasetUploadCadenceGranularity


@dataclass(frozen=True)
class DatasetOptions:
    dataset_cadence: DatasetCadence
    whylabs_upload_cadence: DatasetUploadCadence
    schema: Optional[DatasetSchema] = None


@dataclass(frozen=True)
class LangkitOptions:
    metrics: List[Any] = field(default_factory=list)
    callbacks: List[Any] = field(default_factory=list)
    validators: List[Any] = field(default_factory=list)


@dataclass(frozen=True)
class DatasetKey:
    dataset_id: str
    org_id: str


DatasetId = str


@dataclass(frozen=True)
class ContainerConfiguration:
    langkit_config: Dict[Union[DatasetKey, DatasetId], LangkitOptions] = field(default_factory=dict)
    whylogs_config: Dict[Union[DatasetKey, DatasetId], DatasetOptions] = field(default_factory=dict)


ContainerConfigFn = Callable[[], ContainerConfiguration]
