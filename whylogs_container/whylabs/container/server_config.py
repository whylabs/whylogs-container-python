import json
import logging
import os
from typing import Any, Dict, List, Literal, Optional, Union

from fastapi import HTTPException
from pydantic import BaseModel, Field, field_validator, model_validator
from pydantic.fields import FieldInfo
from pydantic_settings import BaseSettings, EnvSettingsSource, SettingsConfigDict
from typing_extensions import Self

from whylogs_container.whylabs.container.dependencies.api_key_util import parse_api_key
from whylogs_container.whylabs.container.dependencies.header_values import HeaderNames
from whylogs_container_types import DatasetCadence, DatasetUploadCadenceGranularity

_logger = logging.getLogger(__name__)


class CustomSettingsSource(EnvSettingsSource):
    def prepare_field_value(self, field_name: str, field: FieldInfo, value: Any, value_is_complex: bool) -> Any:
        # allow comma-separated list parsing
        if field_name == "auto_pull_whylabs_policy_model_ids":
            return value.split(",") if value else None

        return super().prepare_field_value(field_name, field, value, value_is_complex)


_default_secret_path = "/var/run/secrets/whylabs.ai/env"
_default_secret_json_file = f"{_default_secret_path}/guardrails.json"


class SecretsFile(BaseModel):
    model_config = SettingsConfigDict(frozen=True, env_ignore_empty=True)

    whylabs_api_key: Optional[str] = None
    static_secret: Optional[str] = None


class SecretsConfig(BaseSettings):
    model_config = SettingsConfigDict(frozen=True, env_ignore_empty=True)

    secrets_path_files: str = _default_secret_path
    """
    This is the path to the root of the secret dir. This applies when using individual secret files rather than a single
    json file for all secrets. Non secrets can be loaded from this path as well.
    """

    secrets_path_json: str = _default_secret_json_file
    """
    The path to the secrets json file. This applies when using a single json file for all secrets. This file contains the
    secrets for the container where the following options can be loaded from:

        - `WHYLABS_API_KEY`
        - `STATIC_SECRET`
    """


class ServerConfig(BaseSettings):
    """
    This class has all of the configuration options for the container. Each of the fields of this class can be set via
    instantiating this class in Python or by setting an case insensitive environment variable with the same name.
    """

    model_config = SettingsConfigDict(
        frozen=True, env_ignore_empty=True, secrets_dir=SecretsConfig().secrets_path_files, str_strip_whitespace=True
    )

    @classmethod
    def settings_customise_sources(cls, settings_cls, init_settings, env_settings, dotenv_settings, file_secret_settings):
        return (
            init_settings,
            CustomSettingsSource(settings_cls),
            dotenv_settings,
            file_secret_settings,
        )

    def get_values_safe(self) -> Dict[str, Any]:
        fields: Dict[str, Any] = {}
        for field, value in self.__dict__.items():
            extras = self.model_fields[field].json_schema_extra
            if extras is not None and extras.get("safe_to_print", False) is False:  # pyright: ignore[reportFunctionMemberAccess]
                fields[field] = None if value is None else "***"
            else:
                fields[field] = value

        return fields

    @staticmethod
    def get_secrets_file(secrets_path: str) -> Optional[SecretsFile]:
        if not os.path.isfile(secrets_path):
            _logger.info(f"No secrets file found at {secrets_path}")
            return None

        try:
            # Create a SecretsFile model to validate the secrets file
            with open(secrets_path, "r") as secrets_file:
                secrets = json.load(secrets_file)
                return SecretsFile(**secrets)
        except FileNotFoundError:
            _logger.info(f"Skip loading secret from file. File not found at: {secrets_path}")
        except json.JSONDecodeError:
            _logger.info(f"Error: Invalid JSON format in secrets file {secrets_path}. Skip loading secret from file")
        except Exception as e:
            _logger.info(f"An error occurred. Skip loading secret from file. Error: {e}")

    @property
    def org_id(self) -> str:
        """
        Convenience function to gett the org id from the WhyLabs API key.
        """
        return parse_api_key(self.whylabs_api_key).org_id

    whylabs_api_key: str = Field(
        default="xxxxxxxxxx.xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx:xxxxx", json_schema_extra={"safe_to_print": False}
    )
    """
    Required. API key for WhyLabs.
    """

    whylabs_api_endpoint: str = "https://api.whylabsapp.com"
    """
    The endpoint for WhyLabs API. Defaults to the production endpoint.
    """

    whylabs_api_cache_endpoint: Optional[str] = None
    """
    An endpoint for WhyLabs API cache. If this is set then it will be used for certain WhyLabs requests. This is
    a part of a larger deployment strategy for the container and not intended to be a general customization.
    """

    static_secret: Optional[str] = Field(default=None, json_schema_extra={"safe_to_print": False})
    """
    If set, the container will assume that the auth header sent by clients should match this string exactly. The default behavior
    is to assume that the auth header is an api key and validate it remotely against the WhyLabs platform.
    """

    disable_client_auth: bool = False
    """
    If set to True, the container will not require a api key or secret for requests.
    """

    default_whylabs_dataset_cadence: DatasetCadence = DatasetCadence.HOURLY
    """
    Can be DAILY (default) or HOURLY. This is used whenever the cadence is not
    specified in the dataset schema, or if you don't specify a dataset schema.
    This determines how data is bucketed in the container. Data for each profile
    will be pooled by the hour, minute, or day.
    """

    default_whylabs_upload_cadence: DatasetUploadCadenceGranularity = DatasetUploadCadenceGranularity.MINUTE
    """
    Can be H (default), M, or D. This determines how often profiles are uploaded to WhyLabs.
    """

    default_whylabs_upload_interval: int = 5
    """
    Default interval for WhyLabs uploads. This determines how many units of time (hours, minutes, or days)
    """

    fail_startup_without_config: bool = False
    """
    If set to True, the container will fail to start if no custom configuration is found. This is a safeguard if you
    want to make sure you set up the container correctly and you're using python to configure dataset schemas for
    whylogs.
    """

    fail_startup_without_policies: bool = False
    """
    Just like FAIL_STARTUP_WITHOUT_CONFIG but for yaml policies instead of custom python configuration.
    """

    llm_container: bool = False
    """
    Env var to identify if the deployment is of the type LLM or not. Defaults to False, but set to
    True on LLM image build
    """

    disable_whylabs_policy_polling: bool = True
    """
    Don't poll the WhyLabs api to download policy files. This is useful for a totally offline deployment that uses local policies built
    into the container only.
    """

    s3_config_sync: bool = False
    """
    Optional. If set to True, the container will sync S3 YAML config files with the container and parse them into schemas.
    """

    s3_config_bucket_name: str = ""
    """
    Optional. The name of the S3 bucket where the container will look for YAML files to keep in sync with.
    """

    s3_config_prefix: str = ""
    """
    Optional. The prefix of the S3 bucket where the container will look for YAML files to keep in sync with.
    If not defined, it will look for files on the bucket's root.
    """

    s3_config_sync_interval: int = 15
    """
    Optional. The interval at which the container will sync the S3 bucket's YAML files. This is the
    number of units of time (hours, minutes, or days) that the container will wait before syncing the S3 bucket with container schemas.
    Defaults to 15.
    """

    s3_config_sync_cadence: DatasetUploadCadenceGranularity = DatasetUploadCadenceGranularity.MINUTE
    """
    Optional. The cadence at which the container will sync the S3 bucket's YAML files.
    Can be H (hourly), M (minute), or D (daily).
    Defaults to MINUTE.
    """

    s3_profile_upload_bucket: str
    s3_profile_upload_prefix: Optional[str] = None

    config_sync_interval: int = 1
    """
    Optional. The interval at which the container will sync remote policies. This is the number of units of time (hours,
    minutes, or days) that the container will wait before syncing the remote policies.
    Defaults to 15.
    """

    config_sync_jitter_seconds: int = 5
    """
    Optional. The amount of jitter in seconds to add to the sync interval. This is useful to avoid all containers syncing
    against the local WhyLabs cache instance or the real WhyLabs platform.
    """

    config_sync_cadence: DatasetUploadCadenceGranularity = DatasetUploadCadenceGranularity.MINUTE
    """
    Optional. The cadence at which the container will sync remote policies. Can be H (hourly), M (minute), or D (daily).
    Defaults to MINUTE.
    """

    log_level: int = logging.INFO
    """
    Defaults to INFO. The log level for the container. Can be DEBUG, INFO, WARNING, ERROR, or CRITICAL.
    """

    policy_base_dir: str = "./whylogs_container/whylogs_config/"
    """
    Env var to identify the root directory for policy configs. Defaults to /whylogs_container/whylogs_config/
    """

    trace_endpoint: str = "https://api.whylabsapp.com/v1/traces"
    """
    Optional. The endpoint to send trace data to. If not set, traces are sent to WhyLabs production.
    """

    disable_tracing: bool = False
    """
    Optional. If set to True, the container will not send trace data to WhyLabs.
    """

    disable_profiling: bool = False
    """
    Optional. If set to True, the container will not profile data or upload profiles to WhyLabs.
    """

    auto_pull_whylabs_policy_model_ids: List[str] = []
    """
    Optional. If set, the container will automatically pull policies from WhyLabs for the given model ids.
    """

    aws_sqs_url: Optional[str] = None
    """
    Optional. The URL of the AWS SQS queue to poll for messages.
    """

    aws_sqs_dlq_url: Optional[str] = None
    """
    Optional. The URL of the AWS SQS dead letter queue to send messages that can't be processed.
    """

    max_request_batch_size: int = 50_000
    """
    Optional. The maximum number of profiles that can be sent in a single request. Defaults to 50,000.
    """

    max_request_buffer_bytes: int = 1_000_000_000
    """
    Optional. The maximum number of bytes that can be sent in a single request. Defaults to 1,000,000,000.
    """

    cache_assets_runtime: bool = False
    """
    Optional. If set to True, the container will attempt to download and cache missing assets at runtime. The default is False
    because the container build should be the place where this happens so no runtime downloads are required, but this can be set
    when building custom containers, running tests, etc.
    """

    pre_warm_cache: bool = True
    """
    When True, the first thing the container does is execute a test workflow that forces the download of all missing assets and
    initializes any metrics that it will use, all before the health check.
    """

    mac_dev_mode: bool = False
    """
    Set this to true to develop from a Mac. This will avoid using dependencies that won't work on a Mac at the cost of performance
    and some hackery.
    """

    debug_endpoint_enabled: bool = False
    """
    If set to True, the container will expose a debug endpoint at /debug/evaluate that allows passing custom policies
    in requests. It's convenient for prototyping policies but it has a large memory impact on the main process of the container.
    """

    policy_editor_required: bool = False
    """
    If set to True then the container will fail if it can't find the policy editor project to server in the LLM container version.
    """

    tenancy_mode: Literal["SINGLE", "MULTI"] = "SINGLE"
    f"""
    The tenancy mode of the container. Can be SINGLE or MULTI. Defaults to SINGLE.
    This enables multi organization support in the container, which requires that the api key configured at the container level is
    an organization that manages child organizations, and each of the requests must have a {HeaderNames.whylabs_api_key_header.value}
    header with and api key for one of the child orgs.
    """

    enforce_tenancy_mode: bool = False
    """
    This option is here as a fail safe for unexpected use cases.
    By default, when running in multi-tenancy mode, the container will enforce that all requests have a valid API key for a child
    organization. If this is set to False, the container will not enforce this and will allow any request to pass through. That means
    that the profiles may fail to upload in the background later on if the wrong API key was used by mistake. Don't disable this unless
    there is a good reason.
    """

    @field_validator("log_level", mode="before")
    @classmethod
    def parse_log_level(cls, v: Union[str, int]) -> int:
        if isinstance(v, str):
            return getattr(logging, v.upper())
        return v

    @model_validator(mode="before")
    @classmethod
    def before_validator(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        # Convert each empty string in values to None
        values = {k: None if v == "" else v for k, v in values.items()}

        secrets_config = SecretsConfig()
        secrets_path = secrets_config.secrets_path_json
        if not secrets_path:
            return values

        secrets_file = cls.get_secrets_file(secrets_path)

        if secrets_file:
            values["whylabs_api_key"] = values.get("whylabs_api_key", secrets_file.whylabs_api_key)
            values["static_secret"] = values.get("static_secret", secrets_file.static_secret)

        return values

    @model_validator(mode="after")
    def after_validator(self) -> Self:
        if self.disable_profiling and not self.llm_container:
            raise ValueError(
                "Cannot disable profiling via DISABLE_PROFILING in the whylogs container, it's the primary function of the container."
                " This option is only available in the LLM Secure container variant."
            )

        if self.tenancy_mode == "MULTI":
            _logger.info("Running in multi-tenancy mode")
            if self.aws_sqs_url or self.aws_sqs_dlq_url:
                raise ValueError("SQS is not supported in multi-tenancy mode.")

            if self.auto_pull_whylabs_policy_model_ids:
                raise ValueError(
                    "auto_pull_whylabs_policy_model_ids is not supported in multi-tenancy mode. Policies will be "
                    "automatically pulled from all of the orgs under the parent org defined by the container api key."
                )

        return self

    def auth_disabled(self) -> bool:
        return self.disable_client_auth

    def assert_llm_endpoint(self) -> None:
        if not self.llm_container:
            raise HTTPException(status_code=404, detail="The LLM endpoint is not available on this container")


__config: Optional[ServerConfig] = None


def get_server_config() -> ServerConfig:
    global __config
    if __config is None:
        __config = ServerConfig()  # pyright: ignore[reportCallIssue]
    return __config


def set_server_config(config: ServerConfig) -> None:
    global __config
    __config = config


# Really just for unit tests
def reset_server_config() -> None:
    global __config
    __config = None
