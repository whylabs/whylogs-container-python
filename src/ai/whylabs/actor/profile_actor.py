from functools import reduce
import os
from itertools import groupby
import orjson
from dataclasses import dataclass
from typing import Dict, List, Optional, Type, Union, cast, TypedDict
from ..container.requests import LogRequest, DataTypes
from ..container.config import get_dataset_options, ContainerConfig

import pandas as pd
import whylogs as y
from faster_fifo import Queue
from whylogs.api.logger.rolling import TimedRollingLogger

from .actor import Actor, CloseMessage


class MultipleDict(TypedDict):
    columns: List[str]
    data: List[List[DataTypes]]


class LogRequestDict(TypedDict):
    datasetId: str
    single: Optional[Dict[str, DataTypes]]
    multiple: Optional[MultipleDict]


class DebugMessage:
    pass


class PublishMessage:
    pass


@dataclass
class RawLogMessage:
    request: bytes

    def to_log_request_dict(self) -> LogRequestDict:
        d: LogRequestDict = orjson.loads(self.request)
        return d


MessageType = Union[DebugMessage, PublishMessage, RawLogMessage]

# TODO config structure
# - https://whylabs.github.io/whylogs-container-docs/whylogs-container/ai.whylabs.services.whylogs.core.config/-env-var-names/index.html?query=enum%20EnvVarNames%20:%20Enum%3CEnvVarNames%3E


class ProfileActor(Actor[MessageType]):
    def __init__(self, queue: Queue, env_vars: ContainerConfig = ContainerConfig()) -> None:
        super().__init__(queue)
        # NOTE, this is created before the process forks. You can't access this from the original process via
        # a method. You need some sort of IPC signal.
        self.loggers: Dict[str, TimedRollingLogger] = {}
        self.env_vars = env_vars

    def _create_logger(self, dataset_id: str) -> TimedRollingLogger:
        options = get_dataset_options(dataset_id)
        interval = (
            options and options.whylabs_upload_cadence.interval
        ) or self.env_vars.default_whylabs_upload_interval
        when = (
            options
            and options.whylabs_upload_cadence.granularity.value
            or self.env_vars.default_whylabs_upload_cadence.value
        )

        logger = y.logger(
            mode="rolling", interval=interval, when=when, base_name="profile_", schema=options and options.schema
        )
        logger.append_writer(
            "whylabs",
            org_id=self.env_vars.whylabs_org_id,
            api_key=self.env_vars.whylabs_api_key,
            dataset_id=dataset_id,
        )
        self._logger.info(f"Created logger for {dataset_id} with interval {interval} and upload cadence {when}")
        return logger

    def _get_logger(self, dataset_id: str) -> TimedRollingLogger:
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
            giga_message: LogRequestDict = reduce(_reduce_dicts, group)
            df = log_dict_to_data_frame(giga_message)
            logger.log(df)

    def process_debug_message(self, messages: List[DebugMessage]) -> None:
        # TODO @jamie why does this always return something even after it writes to whylabs? Shouldn't everything be purged locally?
        # TODO repro fully
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
            logger._do_rollover()


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
