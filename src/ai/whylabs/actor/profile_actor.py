from functools import reduce
from whylabs_toolkit.container.config_types import DatasetCadence, DatasetUploadCadenceGranularity
import os
from itertools import groupby
import orjson
from dataclasses import dataclass
from typing import Dict, List, Optional, Type, Union, cast, TypedDict
from ..container.requests import LogRequest, DataTypes
from ..container.config import get_dataset_options, ContainerConfig
from ...util.time import truncate_time_ms, TimeGranularity

import pandas as pd
import whylogs as y
from faster_fifo import Queue
from whylogs.api.writer import Writers
from whylogs.api.logger.experimental.multi_dataset_logger.multi_dataset_rolling_logger import MultiDatasetRollingLogger
from whylogs.api.logger.experimental.multi_dataset_logger.time_util import TimeGranularity as yTimeGranularity 
from whylogs.api.logger.experimental.multi_dataset_logger.time_util import Schedule 

from .actor import Actor, CloseMessage


class MultipleDict(TypedDict):
    columns: List[str]
    data: List[List[DataTypes]]


class LogRequestDict(TypedDict):
    datasetId: str
    timestamp: int
    single: Optional[Dict[str, DataTypes]]
    multiple: Optional[MultipleDict]


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


MessageType = Union[DebugMessage, PublishMessage, RawLogMessage]

# TODO config structure
# - https://whylabs.github.io/whylogs-container-docs/whylogs-container/ai.whylabs.services.whylogs.core.config/-env-var-names/index.html?query=enum%20EnvVarNames%20:%20Enum%3CEnvVarNames%3E


class ProfileActor(Actor[MessageType]):
    def __init__(self, queue: Queue, env_vars: ContainerConfig = ContainerConfig()) -> None:
        super().__init__(queue)
        # NOTE, this is created before the process forks. You can't access this from the original process via
        # a method. You need some sort of IPC signal.
        self.loggers: Dict[str, MultiDatasetRollingLogger] = {}
        self.env_vars = env_vars

    def _create_logger(self, dataset_id: str) -> MultiDatasetRollingLogger:
        options = get_dataset_options(dataset_id)
        upload_interval = (
            options and options.whylabs_upload_cadence.interval
        ) or self.env_vars.default_whylabs_upload_interval
        upload_cadence = (
            options and options.whylabs_upload_cadence.granularity
        ) or self.env_vars.default_whylabs_upload_cadence

        dataset_cadence = (options and options.dataset_cadence) or self.env_vars.default_dataset_cadence

        if dataset_cadence == DatasetCadence.DAILY:
            aggregate_by = yTimeGranularity.Day
        elif dataset_cadence == DatasetCadence.HOURLY:
            aggregate_by = yTimeGranularity.Hour
        else:
            raise Exception(f"Unknown dataset cadence {dataset_cadence}")

        if upload_cadence == DatasetUploadCadenceGranularity.DAY:
            schedule = Schedule(cadence=yTimeGranularity.Day, interval=upload_interval)
        elif upload_cadence == DatasetUploadCadenceGranularity.HOUR:
            schedule = Schedule(cadence=yTimeGranularity.Hour, interval=upload_interval)
        elif upload_cadence == DatasetUploadCadenceGranularity.MINUTE:
            schedule = Schedule(cadence=yTimeGranularity.Minute, interval=upload_interval)

        whylabs_writer = Writers.get(
            "whylabs", org_id=self.env_vars.whylabs_org_id, api_key=self.env_vars.whylabs_api_key, dataset_id=dataset_id
        )
        logger = MultiDatasetRollingLogger(
            aggregate_by=aggregate_by,
            writers=[whylabs_writer],
            schema=options and options.schema,
            write_schedule=schedule,
        )

        self._logger.info(
            f"Created logger for {dataset_id} with interval {upload_interval} and upload cadence {upload_cadence}"
        )
        return logger

    def _get_logger(self, dataset_id: str) -> MultiDatasetRollingLogger:
        if not dataset_id in self.loggers:
            self.loggers[dataset_id] = self._create_logger(dataset_id)
        return self.loggers[dataset_id]

    async def process_batch(self, batch: List[MessageType], batch_type: Type) -> None:
        if batch_type == DebugMessage:
            self.process_debug_message(cast(List[DebugMessage], batch))
        elif batch_type == PublishMessage:
            self.process_publish_message(cast(List[PublishMessage], batch))
        elif batch_type == RawLogMessage:
            self.process_log_dicts(cast(List[RawLogMessage], batch))
        elif batch_type == CloseMessage:
            self.process_close_message(cast(List[CloseMessage], batch))
        else:
            raise Exception(f"Unknown message type {batch_type}")

    def process_close_message(self, messages: List[CloseMessage]) -> None:
        self._logger.info("Running pre shutdown operations")
        self._logger.info(f"Closing down {len(self.loggers)} loggers")
        for datasetId, logger in self.loggers.items():
            self._logger.info(f"Closing whylogs logger for {datasetId}")
            logger.close()

    def process_log_dicts(self, messages: List[RawLogMessage]) -> None:
        self._logger.info("Processing log request message")
        log_dicts = [m.to_log_request_dict() for m in messages]

        for dataset_id, group in groupby(log_dicts, lambda it: it["datasetId"]):
            logger = self._get_logger(dataset_id)
            options = get_dataset_options(dataset_id)
            cadence = (options and options.dataset_cadence) or self.env_vars.default_dataset_cadence

            for dataset_timestamp, sub_group in groupby(group, lambda it: determine_dataset_timestamp(cadence, it)):
                self._logger.info(f"Logging data for ts {dataset_timestamp} in dataset {dataset_id}")
                giga_message: LogRequestDict = reduce(_reduce_dicts, sub_group)
                df = log_dict_to_data_frame(giga_message)
                logger.log(df, timestamp_ms=dataset_timestamp, sync=True)

    def process_debug_message(self, messages: List[DebugMessage]) -> None:
        for dataset_id, logger in self.loggers.items():
            profiles = logger._get_matching_profiles()
            for profile in profiles:
                self._logger.info(f"{dataset_id}{os.linesep}{profile.view().to_pandas()}")

    def process_publish_message(self, messages: Optional[List[PublishMessage]] = None) -> None:
        if not self.loggers:
            self._logger.debug(f"No profiles to publish")
            return

        self._logger.debug(f"Force publishing profiles")
        for dataset_id, logger in self.loggers.items():
            self._logger.info(f"Force rolling dataset {dataset_id}")
            logger.flush()


def log_request_to_data_frame(request: LogRequest) -> pd.DataFrame:
    if request.single:
        return pd.DataFrame.from_dict(request.single)
    elif request.multiple:
        return pd.DataFrame(request.multiple.data, columns=request.multiple.columns)


def log_dict_to_data_frame(request: LogRequestDict) -> pd.DataFrame:
    if "single" in request and request["single"] is not None:
        return pd.DataFrame.from_dict(request["single"])
    elif "multiple" in request and request["multiple"] is not None:
        return pd.DataFrame(request["multiple"]["data"], columns=request["multiple"]["columns"])
    else:
        raise Exception(f"Request missing both the single and multiple fields {request}")


# TODO this isn't technically correct all the time. It depends on the columns being the same. The "correct" way
# would be to further group by column configuration but that would be a lot more work. Need to figure out a nice way to fix.
def _reduce_dicts(acc: LogRequestDict, cur: LogRequestDict) -> LogRequestDict:
    if not acc["multiple"]:
        raise Exception("no")
    if not cur["multiple"]:
        raise Exception("no")

    acc["multiple"]["data"].extend(cur["multiple"]["data"])
    return acc


def determine_dataset_timestamp(cadence: DatasetCadence, request: LogRequestDict) -> int:
    ts = request["timestamp"]
    return truncate_time_ms(ts, TimeGranularity.D if cadence == DatasetCadence.DAILY else TimeGranularity.H)
