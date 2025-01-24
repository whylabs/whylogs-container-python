from whylogs.api.logger.experimental.logger.actor.thread_rolling_logger import TimeGranularity
from whylogs.api.logger.experimental.logger.actor.time_util import Schedule
from whylogs_container_types import DatasetCadence, DatasetUploadCadenceGranularity


def convert_schedule(cadence: DatasetUploadCadenceGranularity, interval: int) -> Schedule:
    if cadence == DatasetUploadCadenceGranularity.DAY:
        return Schedule(cadence=TimeGranularity.Day, interval=interval)
    elif cadence == DatasetUploadCadenceGranularity.HOUR:
        return Schedule(cadence=TimeGranularity.Hour, interval=interval)
    elif cadence == DatasetUploadCadenceGranularity.MINUTE:
        return Schedule(cadence=TimeGranularity.Minute, interval=interval)
    else:
        raise Exception(f"Unknown upload cadence {cadence}")


def convert_dataset_cadence(dataset_cadence: DatasetCadence) -> TimeGranularity:
    if dataset_cadence == DatasetCadence.DAILY:
        return TimeGranularity.Day
    elif dataset_cadence == DatasetCadence.HOURLY:
        return TimeGranularity.Hour
    else:
        raise Exception(f"Unknown dataset cadence {dataset_cadence}")
