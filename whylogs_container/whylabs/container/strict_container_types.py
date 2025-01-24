from dataclasses import dataclass, field
from typing import Dict, List, Union

from langkit.core.metric import MetricCreator
from langkit.core.validation import Validator
from langkit.core.workflow import Callback
from whylogs_container_types import DatasetId, DatasetKey, DatasetOptions


@dataclass(frozen=True)
class StrictLangkitOptions:
    metrics: List[MetricCreator] = field(default_factory=list)
    callbacks: List[Callback] = field(default_factory=list)
    validators: List[Validator] = field(default_factory=list)


@dataclass(frozen=True)
class StrictContainerConfiguration:
    langkit_config: Dict[Union[DatasetKey, DatasetId], StrictLangkitOptions] = field(default_factory=dict)
    whylogs_config: Dict[Union[DatasetKey, DatasetId], DatasetOptions] = field(default_factory=dict)
