from typing import Optional, Dict
import os
from whylogs.core.schema import DatasetSchema
import logging
from enum import Enum
from whylabs_toolkit.container.config_types import DatasetCadence, DatasetOptions, DatasetUploadCadenceGranularity


_logger = logging.getLogger("config")


class EnvVarNames(Enum):
    WHYLABS_API_KEY = "WHYLABS_API_KEY"
    WHYLABS_ORG_ID = "WHYLABS_ORG_ID"
    DEFAULT_WHYLABS_DATASET_CADENCE = "DEFAULT_WHYLABS_DATASET_CADENCE"
    DEFAULT_WHYLABS_UPLOAD_CADENCE = "DEFAULT_WHYLABS_UPLOAD_CADENCE"
    DEFAULT_WHYLABS_UPLOAD_INTERVAL = "DEFAULT_WHYLABS_UPLOAD_INTERVAL"

    FAIL_STARTUP_WITHOUT_CONFIG = "FAIL_STARTUP_WITHOUT_CONFIG"


class ContainerConfig:
    whylabs_api_key: str
    whylabs_org_id: str

    default_dataset_cadence: DatasetCadence
    default_whylabs_upload_cadence: DatasetUploadCadenceGranularity
    default_whylabs_upload_interval: int

    fail_startup_without_config: bool

    def __init__(self) -> None:
        self.whylabs_api_key = self._require_env(EnvVarNames.WHYLABS_API_KEY)
        self.whylabs_org_id = self._require_env(EnvVarNames.WHYLABS_ORG_ID)

        self.fail_startup_without_config = self._read_env(EnvVarNames.FAIL_STARTUP_WITHOUT_CONFIG) == "True"

        default_dataset_cadence = self._read_env(EnvVarNames.DEFAULT_WHYLABS_DATASET_CADENCE)
        self.default_dataset_cadence = (
            default_dataset_cadence and DatasetCadence[default_dataset_cadence] or DatasetCadence.DAILY
        )

        default_whylabs_upload_cadence = self._read_env(EnvVarNames.DEFAULT_WHYLABS_UPLOAD_CADENCE)
        self.default_whylabs_upload_cadence = (
            default_whylabs_upload_cadence
            and DatasetUploadCadenceGranularity[default_whylabs_upload_cadence]
            or DatasetUploadCadenceGranularity.HOUR
        )

        default_whylabs_upload_interval = self._read_env(EnvVarNames.DEFAULT_WHYLABS_UPLOAD_INTERVAL)
        self.default_whylabs_upload_interval = (
            default_whylabs_upload_interval and int(default_whylabs_upload_interval) or 1
        )

    def _require_env(self, var: EnvVarNames) -> str:
        try:
            return os.environ[var.name]
        except KeyError as e:
            _logger.error(f"Missing value for env var {var.name}")
            raise e

    def _read_env(self, var: EnvVarNames) -> Optional[str]:
        try:
            return os.environ[var.name]
        except KeyError:
            return None


def _load_custom_options() -> Optional[Dict[str, DatasetOptions]]:
    config = ContainerConfig()
    try:
        # This may or may not exist depending on if the use supplies custom configuration and
        # builds it into a downstream docker image
        from ...whylogs_config.config import schemas

        s: Dict[str, DatasetOptions] = schemas  # TODO why is this cast required?
        _logger.info(f"Found custom whylogs configuration {s}")
        return s
    except (ImportError, ModuleNotFoundError) as e:
        _logger.warning("No custom whylogs configuration found.")
        if config.fail_startup_without_config:
            _logger.exception(
                "Failing startup because no custom whylogs configuration was found and FAIL_STARTUP_WITHOUT_CONFIG env var is True",
                e,
            )
            os._exit(1)
        return None
    except Exception as e:
        _logger.exception("Couldn't import custom configuration", e)
        raise e


_schemas: Dict[str, DatasetOptions] = _load_custom_options() or {}


def get_dataset_options(dataset_id: str) -> Optional[DatasetOptions]:
    try:
        options = _schemas[dataset_id]
        return options
    except KeyError:
        _logger.info(f"No custom configuration for dataset {dataset_id}")
        return None
