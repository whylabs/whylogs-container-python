import os
import time
from functools import reduce
from itertools import groupby
from typing import Dict, List, Optional, Type, Union, cast

from faster_fifo import Queue
from whylabs_toolkit.container.config_types import DatasetCadence, DatasetUploadCadenceGranularity
from whylogs.api.logger.experimental.multi_dataset_logger.multi_dataset_rolling_logger import MultiDatasetRollingLogger
from whylogs.api.logger.experimental.multi_dataset_logger.time_util import Schedule
from whylogs.api.logger.experimental.multi_dataset_logger.time_util import TimeGranularity as yTimeGranularity
from whylogs.api.writer import Writers

from ...util.string_util import encode_strings
from ..container.config import ContainerConfig, get_dataset_options
from .actor import Actor, CloseMessage
from .profile_actor_messages import (
    DebugMessage,
    LogEmbeddingRequestDict,
    LogRequestDict,
    PublishMessage,
    RawLogEmbeddingsMessage,
    RawLogMessage,
    determine_dataset_timestamp,
    get_columns,
    get_embeddings_columns,
    log_dict_to_data_frame,
    log_dict_to_embedding_matrix,
    reduce_embeddings_request,
    reduce_log_requests,
)

MessageType = Union[DebugMessage, PublishMessage, RawLogMessage, RawLogEmbeddingsMessage]


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

    def process_batch(self, batch: List[MessageType], batch_type: Type) -> None:
        if batch_type == DebugMessage:
            self.process_debug_message(cast(List[DebugMessage], batch))
        elif batch_type == PublishMessage:
            self.process_publish_message(cast(List[PublishMessage], batch))
        elif batch_type == RawLogMessage:
            self.process_log_dicts(cast(List[RawLogMessage], batch))
        elif batch_type == RawLogEmbeddingsMessage:
            self.process_log_embeddings_dicts(cast(List[RawLogEmbeddingsMessage], batch))
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

    def process_log_embeddings_dicts(self, messages: List[RawLogEmbeddingsMessage]) -> None:
        self._logger.info("Processing log embeddings request message")
        log_dicts = [m.to_log_embeddings_request_dict() for m in messages]

        for dataset_id, group in groupby(log_dicts, lambda it: it["datasetId"]):
            logger = self._get_logger(dataset_id)
            options = get_dataset_options(dataset_id)
            cadence = (options and options.dataset_cadence) or self.env_vars.default_dataset_cadence

            for dataset_timestamp, ts_grouped in groupby(group, lambda it: determine_dataset_timestamp(cadence, it)):
                for n, sub_group in groupby(ts_grouped, lambda it: encode_strings(get_embeddings_columns(it))):
                    self._logger.info(
                        f"Logging embeddings for ts {dataset_timestamp} in dataset {dataset_id} for column set {n}"
                    )
                    giga_message: LogEmbeddingRequestDict = reduce(reduce_embeddings_request, sub_group)
                    row = log_dict_to_embedding_matrix(giga_message)

                    row_count = 0
                    for embeddings in row.values():
                        row_count += len(embeddings)

                    start = time.perf_counter()
                    logger.log(row, timestamp_ms=dataset_timestamp, sync=True)
                    self._logger.debug(
                        f'Took {time.perf_counter() - start}s to log {row_count} rows for {len(giga_message["embeddings"])} columns'
                    )

    def process_log_dicts(self, messages: List[RawLogMessage]) -> None:
        self._logger.info("Processing log request message")
        log_dicts = [m.to_log_request_dict() for m in messages]

        for dataset_id, group in groupby(log_dicts, lambda it: it["datasetId"]):
            logger = self._get_logger(dataset_id)
            options = get_dataset_options(dataset_id)
            cadence = (options and options.dataset_cadence) or self.env_vars.default_dataset_cadence

            for dataset_timestamp, ts_grouped in groupby(group, lambda it: determine_dataset_timestamp(cadence, it)):
                for n, sub_group in groupby(ts_grouped, lambda it: encode_strings(get_columns(it))):
                    self._logger.info(
                        f"Logging data for ts {dataset_timestamp} in dataset {dataset_id} for column set {n}"
                    )
                    giga_message: LogRequestDict = reduce(reduce_log_requests, sub_group)
                    df = log_dict_to_data_frame(giga_message)
                    start = time.perf_counter()
                    logger.log(df, timestamp_ms=dataset_timestamp, sync=True)
                    self._logger.debug(f"Took {time.perf_counter() - start}s to log {len(df.index)}")

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
