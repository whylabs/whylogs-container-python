from dataclasses import dataclass, field
from typing import Dict, List, Literal, Optional, Union

from pydantic import BaseModel


@dataclass(frozen=True)
class ValidationMetadata:
    # TODO there shouldn't be two levels of metric concept. We should flatten this down so that
    # we have "reading_ease" instad of "textstat" and "reading_ease".
    validator_name: Optional[str] = field(default=None)
    failed_metric: Optional[str] = field(default=None)
    value: Optional[Union[str, int, float]] = field(default=None)
    timestamp: Optional[int] = field(default=None)
    is_valid: Optional[bool] = field(default=False)


@dataclass(frozen=True)
class ValidationReport:
    failures: List[ValidationMetadata]


@dataclass
class ConfigurationMetadata:
    config_error: bool
    timestamp: int
    message: str


ModuleNames = Literal["toxicity", "sentiment", "themes", "textstat", "regexes", "input_output"]


class ModuleDefinition(BaseModel):
    module: ModuleNames
    upper_threshold: Optional[Union[float, int]] = None
    lower_threshold: Optional[Union[float, int]] = None
    include_upper: Optional[bool] = None
    config_path: Optional[str] = None
    metric: Optional[str] = None


ValidationRules = Dict[Literal["prompt", "response"], List[ModuleDefinition]]


class YamlConfig(BaseModel):
    whylabs_dataset_id: str
    policy: str
    id: str
    policy_version: str
    schema_version: str
    rules: ValidationRules = {}
    profiling: List[Literal[ModuleNames, "all"]] = []
