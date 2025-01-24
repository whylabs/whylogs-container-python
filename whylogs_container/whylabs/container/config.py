import logging
import random
import sys
import threading
import uuid
from abc import ABC, abstractmethod
from concurrent.futures import Future
from dataclasses import dataclass, field
from datetime import datetime
from functools import cache
from multiprocessing.managers import BaseManager, NamespaceProxy  # pyright: ignore[reportAttributeAccessIssue, reportUnknownVariableType]
from pathlib import Path
from pprint import pformat
from typing import Dict, List, Mapping, Optional, Tuple, Union, cast

import pandas as pd

from langkit.core.workflow import Workflow
from whylogs.api.logger.experimental.logger.actor.actor import CloseMessage, QueueConfig
from whylogs.api.logger.experimental.logger.actor.future_util import wait_result
from whylogs.api.logger.experimental.logger.actor.pipe_signaler import PipeSignaler
from whylogs.api.logger.experimental.logger.actor.process_actor import ProcessActor
from whylogs.api.logger.experimental.logger.actor.process_rolling_logger import QueueType
from whylogs.api.logger.experimental.logger.actor.time_util import FunctionTimer
from whylogs.core.schema import DeclarativeSchema
from whylogs.experimental.core.udf_schema import NO_FI_RESOLVER
from whylogs_container.whylabs.container.policy import Policy, get_policy_org_id
from whylogs_container.whylabs.container.server_config import get_server_config
from whylogs_container.whylabs.container.strict_container_types import StrictLangkitOptions
from whylogs_container.whylabs.container.whylabs_client import get_whylabs_client
from whylogs_container.whylabs.time_utils.time_convert_utils import convert_schedule
from whylogs_container_types import (
    ContainerConfiguration,
    DatasetKey,
    DatasetOptions,
    DatasetUploadCadence,
    LangkitOptions,
)

_logger = logging.getLogger(__name__)

_DEFAULT_DATASET_ID = "default"
_DEFAULT_POLICY_KEY = DatasetKey(dataset_id=_DEFAULT_DATASET_ID, org_id=_DEFAULT_DATASET_ID)


def _load_local_policies() -> List[Policy]:
    server_config = get_server_config()
    if not server_config.llm_container:
        return []

    llm_config_path = server_config.policy_base_dir

    from whylogs_container.whylabs.container.policy.policy_parsing import load_policy_from_str

    policies: List[Policy] = []
    yml_files = list(Path(llm_config_path).glob("*.yml"))
    yaml_files = list(Path(llm_config_path).glob("*.yaml"))
    for file_path in yml_files + yaml_files:
        with open(file_path, "r", encoding="utf-8") as file:
            _logger.info(f"Loading policy from {file_path}")
            file_content: Optional[str] = None
            file_content = file.read()
            try:
                policy = load_policy_from_str(file_content)
            except Exception as e:
                raise Exception(f"Failed to load policy from {file_path}") from e

            policies.append(policy)

    if server_config.fail_startup_without_policies and not policies:
        raise Exception("Failing startup because no custom LLM policies were found and FAIL_STARTUP_WITHOUT_POLICIES env var is True")

    # validate the policies as well
    # make sure there are no duplicate whylogs_dataset_ids
    dataset_ids = [DatasetKey(p.whylabs_dataset_id, get_policy_org_id(p)) for p in policies]
    if len(dataset_ids) != len(set(dataset_ids)):
        raise Exception(f"Duplicate whylabs_dataset_id in policies: {dataset_ids}")

    _logger.info("Loaded local policies: %s", pformat(policies, indent=2, width=200))
    return policies


def _load_s3_policies() -> List[Policy]:
    server_config = get_server_config()
    if not server_config.llm_container or not server_config.s3_config_sync:
        return []

    from whylogs_container.whylabs.container.aws.s3_sync import S3Sync

    policies = S3Sync().sync_s3_policies()
    _logger.info("Loaded s3 policies: %s", pformat(policies, indent=2, width=200))
    return policies


def _load_whylabs_policies() -> List[Policy]:
    server_config = get_server_config()
    if not server_config.llm_container or server_config.disable_whylabs_policy_polling:
        return []

    from whylogs_container.whylabs.container.policy.policy_parsing import load_policy_from_str

    client = get_whylabs_client()
    policy_strings: List[str] = []

    if server_config.auto_pull_whylabs_policy_model_ids:
        _logger.info("Downloading policies for models: %s", server_config.auto_pull_whylabs_policy_model_ids)
        for model_id in server_config.auto_pull_whylabs_policy_model_ids:
            policy_str = client.get_policy(model_id)
            if policy_str:
                policy_strings.append(policy_str)
    elif server_config.tenancy_mode == "MULTI":
        orgs = client.get_managed_orgs()
        _logger.info("Downloading policies for managed orgs: %s", orgs)
        for org in orgs:
            org_policies = client.get_org_policies(org)
            for org_policy in org_policies:
                policy_strings.append(org_policy)
    else:
        org_policies = client.get_org_policies()
        _logger.info("Downloading all model policies for container org")
        for org_policy in org_policies:
            policy_strings.append(org_policy)

    policies: List[Policy] = []
    policy = ""
    try:
        for policy in policy_strings:
            policy = load_policy_from_str(policy)
            policies.append(policy)
    except Exception as e:
        _logger.exception(f"Failed to parse policy {policy}", e)
        raise e

    _logger.info("Loaded WhyLabs policies: %s", pformat(policies, indent=2, width=200))
    return policies


@dataclass(frozen=True)
class ConfigState:
    # Primitive config options. Everything else can be derived from these
    local_policies: Tuple[Policy, ...] = field(default_factory=tuple)
    s3_policies: Tuple[Policy, ...] = field(default_factory=tuple)
    whylabs_policies: Tuple[Policy, ...] = field(default_factory=tuple)
    last_update_time_ms: Optional[int] = None
    # container_config: Optional[ContainerConfiguration] = None

    # metadata for debugging
    create_time: datetime = field(default_factory=datetime.now)

    @cache
    def get_container_config(self) -> Optional[ContainerConfiguration]:
        try:
            # This may or may not exist depending on if the user supplies custom configuration and
            # builds it into a downstream docker image
            from whylogs_container.whylogs_config.config import (  # pyright: ignore[reportMissingImports]
                get_config,  # pyright: ignore[reportUnknownVariableType]
            )

            server_config = cast(ContainerConfiguration, get_config())
            _logger.info(f"Found custom configuration {server_config}")

            # Normalize all the keys. If its a string then convert it into a DatasetKey
            normalized_metric_config: Dict[Union[DatasetKey, str], LangkitOptions] = {}
            for key, value in server_config.langkit_config.items():
                if isinstance(key, str):
                    normalized_metric_config[DatasetKey(dataset_id=key, org_id=get_server_config().org_id)] = value
                else:
                    normalized_metric_config[key] = value

            return ContainerConfiguration(
                langkit_config=normalized_metric_config,
                whylogs_config=server_config.whylogs_config,
            )
        except ModuleNotFoundError as e:
            _logger.warning("No custom python configuration found.")
            server_config = get_server_config()
            if server_config.fail_startup_without_config:
                _logger.exception(
                    "Failing startup because no custom configuration was found and FAIL_STARTUP_WITHOUT_CONFIG env var is True",
                    e,
                )
                sys.exit(1)
            return None
        except Exception as e:
            _logger.exception("Couldn't import custom configuration", e)
            raise e

    @cache
    def get_policies(self) -> Mapping[DatasetKey, Policy]:
        if not self.local_policies + self.s3_policies + self.whylabs_policies:
            return {}

        policies: Dict[DatasetKey, Policy] = {}
        for policy in self.local_policies:
            key = DatasetKey(dataset_id=policy.whylabs_dataset_id, org_id=get_policy_org_id(policy))
            policies[key] = policy

        # S3 policies take precedence over local policies
        for policy in self.s3_policies:
            key = DatasetKey(dataset_id=policy.whylabs_dataset_id, org_id=get_policy_org_id(policy))
            policies[key] = policy

        # WhyLabs policies take precedence over all others
        for policy in self.whylabs_policies:
            key = DatasetKey(dataset_id=policy.whylabs_dataset_id, org_id=get_policy_org_id(policy))
            policies[key] = policy

        return policies

    @cache
    def get_policy_langkit_metric_options(self) -> Mapping[DatasetKey, StrictLangkitOptions]:
        """
        Returns the langkit options derived from all of the known policy files, as though
        they were originally defined in the custom configuration.
        """
        options = {k: v.to_langkit_options() for k, v in self.get_policies().items()}

        _logger.debug("Loaded metric policy options: %s", pformat(options, indent=2, width=200))
        return options

    @cache
    def get_merged_metric_options(self) -> Mapping[DatasetKey, StrictLangkitOptions]:
        """
        Get a map from dataset id to the merged langkit options from the container configuration and all
        sources of the policy files: local, s3, whylabs, etc.
        """
        container_config = self.get_container_config()
        langkit_container_config_options = container_config.langkit_config if container_config else {}
        policy_container_config_options = self.get_policy_langkit_metric_options()

        dataset_ids = set(langkit_container_config_options.keys()) | set(policy_container_config_options.keys())

        langkit_options: Dict[DatasetKey, StrictLangkitOptions] = {}
        for dataset in dataset_ids:
            org_id = dataset.org_id if isinstance(dataset, DatasetKey) else get_server_config().org_id
            key = DatasetKey(dataset_id=dataset, org_id=org_id) if isinstance(dataset, str) else dataset
            option = langkit_container_config_options.get(key) or LangkitOptions()
            policy_option = policy_container_config_options.get(key) or LangkitOptions()

            _logger.debug("Creating merged options for dataset %s", key)
            _logger.debug("option: %s", option)
            _logger.debug("policy_option: %s", policy_option)

            # TODO validate that these are valid langkit objects. They're just Any in the container types
            option = cast(StrictLangkitOptions, option)
            policy_option = cast(StrictLangkitOptions, policy_option)

            langkit_options[key] = StrictLangkitOptions(
                metrics=option.metrics + policy_option.metrics,
                validators=option.validators + policy_option.validators,
                callbacks=option.callbacks + policy_option.callbacks,
            )

        return langkit_options

    @cache
    def get_langkit_metric_workflows(self) -> Mapping[DatasetKey, Workflow]:
        """
        Geta map of dataset id to a workflow that represents the combined langkit options between the container
        configuration and the yaml policy files from local, s3, and whylabs platform.
        """

        merged_metric_options = self.get_merged_metric_options()
        server_config = get_server_config()
        workflows: Dict[DatasetKey, Workflow] = {}
        for dataset, options in merged_metric_options.items():
            workflows[dataset] = Workflow(
                metrics=options.metrics,
                validators=options.validators,
                callbacks=options.callbacks,
                cache_assets=server_config.cache_assets_runtime,
            )

        return workflows

    @cache
    def get_default_langkit_workflow(self) -> Workflow:
        from whylogs_container.whylabs.container.metrics.cached_metrics import get_metric_names

        metric_names = get_metric_names()

        return Workflow(
            metrics=[
                metric_names["prompt.pii"],
                metric_names["prompt.stats.char_count"],
                metric_names["prompt.stats.token_count"],
                metric_names["prompt.similarity.injection"],
                metric_names["response.pii"],
                metric_names["response.stats.token_count"],
                metric_names["response.stats.char_count"],
                metric_names["response.stats.flesch_reading_ease"],
                metric_names["response.sentiment.sentiment_score"],
                metric_names["response.toxicity.toxicity_score"],
                metric_names["response.similarity.refusal"],
            ],
            cache_assets=False,
        )

    def get_default_whylogs_schema(self) -> DatasetOptions:
        server_config = get_server_config()
        schema = DeclarativeSchema(NO_FI_RESOLVER)

        return DatasetOptions(
            schema=schema,
            dataset_cadence=server_config.default_whylabs_dataset_cadence,
            whylabs_upload_cadence=DatasetUploadCadence(
                interval=server_config.default_whylabs_upload_interval,
                granularity=server_config.default_whylabs_upload_cadence,
            ),
        )

    def get_policy(self, dataset_id: str, org_id: str, return_default: bool = True) -> Optional[Policy]:
        policy = self.get_policies().get(DatasetKey(dataset_id=dataset_id, org_id=org_id))
        if policy:
            return policy

        if return_default:
            # This represents the default policy for this organization
            org_default = self.get_policies().get(DatasetKey(dataset_id=_DEFAULT_DATASET_ID, org_id=org_id))

            if org_default:
                return org_default

            # This represents the global default policy
            default = self.get_policies().get(_DEFAULT_POLICY_KEY)

            if default:
                return default

        return None

    def get_langkit_workflow(self, dataset_id: str, org_id: str) -> Workflow:
        options = self.get_langkit_metric_workflows()
        wf = options.get(DatasetKey(dataset_id=dataset_id, org_id=org_id))
        if wf:
            return wf

        org_default_wf = options.get(DatasetKey(dataset_id=_DEFAULT_DATASET_ID, org_id=org_id))
        if org_default_wf:
            return org_default_wf

        default_wf = options.get(_DEFAULT_POLICY_KEY)
        if default_wf:
            return default_wf

        return self.get_default_langkit_workflow()

    @cache
    def get_langkit_score_workflow(self, dataset_id: str, org_id: str) -> Optional[Workflow]:
        """
        Get the langkit workflow for calculating scores based on policy rulesets. This workflow doesn't compute the metrics,
        it consumes the output of another workflow that computes metrics and geneates normalized scores.
        """
        from whylogs_container.whylabs.container.policy.scores import create_score_workflow

        # Check the merged metric options to see if there is any custom python configuration defined for this dataset.
        options = self.get_merged_metric_options().get(DatasetKey(dataset_id=dataset_id, org_id=org_id))

        # Custom configs can't define rulesets yet but if one is present then we want to avoid falling back to the default rulesets
        use_default = options is None

        # Rulesets can only be defined in policy files atm
        policy = self.get_policy(dataset_id, return_default=use_default, org_id=org_id)
        if not policy:
            return None

        rulesets = policy.get_rulesets()

        if not rulesets:
            return None

        return create_score_workflow(rulesets)

    def get_dataset_options(self, dataset_id: str, org_id: str) -> DatasetOptions:
        container_config = self.get_container_config()

        if container_config and dataset_id in container_config.whylogs_config:
            return container_config.whylogs_config[dataset_id]

        key = DatasetKey(dataset_id=dataset_id, org_id=org_id)
        if container_config and key in container_config.whylogs_config:
            return container_config.whylogs_config[key]

        # Fall back to getting the org default options
        if container_config and _DEFAULT_DATASET_ID in container_config.whylogs_config:
            return container_config.whylogs_config[_DEFAULT_DATASET_ID]

        org_default_key = DatasetKey(dataset_id=_DEFAULT_DATASET_ID, org_id=org_id)
        if container_config and org_default_key in container_config.whylogs_config:
            return container_config.whylogs_config[org_default_key]

        # Fall back to getting the default options

        if container_config and _DEFAULT_POLICY_KEY in container_config.whylogs_config:
            return container_config.whylogs_config[_DEFAULT_POLICY_KEY]

        return self.get_default_whylogs_schema()


class ConfigManager(BaseManager):
    pass


class ConfigProxy(NamespaceProxy):  # pyright: ignore[reportUntypedBaseClass]
    _exposed_ = ("__getattribute__", "__setattr__", "__delattr__")


@dataclass
class ConfigInstance:
    config: ConfigState
    """
    The current configuration state. This can only be updated by the ConfigActor in order
    to handle concurrency properly with multiple processes.
    """


@dataclass
class RefreshMessage:
    id: str = field(default_factory=lambda: str(uuid.uuid4()))


ConfigManager.register("ConfigInstance", ConfigManager, ConfigProxy)


class SynchronousConfig(ABC):
    def refresh(self) -> None:
        message = RefreshMessage()
        future: Future[None] = Future()
        self._get_pipe_signaler().register(future, message.id)
        self._get_actor().send(message)
        wait_result(future, timeout=20.0)

    @abstractmethod
    def _get_actor(self) -> "ConfigActor":
        pass

    @abstractmethod
    def start_process(self, pre_warm: bool = False) -> None:
        pass

    @abstractmethod
    def close_process(self) -> None:
        pass

    @abstractmethod
    def _get_pipe_signaler(self) -> PipeSignaler[None]:
        pass

    @abstractmethod
    def is_process_alive(self) -> bool:
        pass


class ConfigActor(ProcessActor[RefreshMessage, None], SynchronousConfig):
    _config_instance: ConfigInstance

    def __init__(self, config_instance: ConfigInstance) -> None:
        queue_config = QueueConfig(
            max_batch_size=1, message_accumualtion_duration=0.001, message_poll_wait=0.005, max_buffer_bytes=100_000_000
        )
        settings = get_server_config()
        super().__init__(
            sync_enabled=True,
            queue_config=queue_config,
            queue_type=QueueType.FASTER_FIFO if not settings.mac_dev_mode else QueueType.MP,
        )
        self._refresh_timer: Optional[FunctionTimer] = None
        self._config_instance = config_instance

    def process_batch(self, batch, batch_type) -> None:
        if batch_type == RefreshMessage:
            self.handle_refresh_message(cast(List[RefreshMessage], batch))
        elif batch_type == CloseMessage:
            self.process_close_message(cast(List[CloseMessage], batch))
        else:
            raise Exception(f"Unknown message type {batch_type}")

    def handle_refresh_message(self, messages: List[RefreshMessage]) -> None:
        try:
            local_policies = _load_local_policies()
            s3_policies = _load_s3_policies()
            whylabs_policies = _load_whylabs_policies()
            update_time = datetime.now()

            config = ConfigState(
                local_policies=tuple(local_policies),
                s3_policies=tuple(s3_policies),
                whylabs_policies=tuple(whylabs_policies),
                last_update_time_ms=int(update_time.timestamp() * 1000),
            )

            # We have to avoid evaluating anything that creates langkit metrics because we don't want
            # any of those libraries to be loaded into memory in the config process.

            config.get_policy_langkit_metric_options()  # Force evaluation of policy parsing
            self._config_instance.config = config
            for message in messages:
                self._get_pipe_signaler().signal((message.id, None, None))
        except Exception as e:
            _logger.exception("Failed to refresh config", e)
            for message in messages:
                self._get_pipe_signaler().signal((message.id, e, None))

    def process_close_message(self, _messages: List[CloseMessage]) -> None:
        if self._refresh_timer:
            self._logger.info("Stopping config refresh timer")
            self._refresh_timer.stop()

    def _refresh_with_jitter(self) -> None:
        server_config = get_server_config()
        jitter = server_config.config_sync_jitter_seconds

        # pick a random millisecond between 0 and jitter
        ms_to_wait = 0 if jitter == 0 else random.randint(0, jitter * 1000)

        self._logger.debug(f"Waiting {ms_to_wait} ms before refreshing config")

        # launch a thread that waits for the jitter time before refreshing
        threading.Timer(ms_to_wait / 1000, lambda: self.send(RefreshMessage()) if self.is_alive() else None).start()

    def run(self) -> None:
        server_config = get_server_config()
        if server_config.llm_container and (server_config.s3_config_sync or not server_config.disable_whylabs_policy_polling):
            # use the newer env var if set
            cadence = server_config.config_sync_cadence or server_config.s3_config_sync_cadence
            interval = server_config.config_sync_interval or server_config.s3_config_sync_interval
            self._refresh_timer = FunctionTimer(
                schedule=convert_schedule(cadence=cadence, interval=interval),
                fn=self._refresh_with_jitter,
            )

        super().run()

    def _get_pipe_signaler(self) -> PipeSignaler[None]:
        if not self._pipe_signaler:
            raise Exception("PipeSignaler not initialized")

        return self._pipe_signaler

    @cache
    def _warm_langkit_metrics(self):
        from whylogs_container.whylabs.container.metrics.cached_metrics import get_metric_names

        dummy_data_frame = pd.DataFrame(
            {
                "prompt": ["hello"],
                "response": ["hello"],
            }
        )

        wf = Workflow(
            metrics=list(get_metric_names().values()),
            cache_assets=False,
        )

        wf.run(dummy_data_frame)

    def start_process(self, pre_warm: bool = False) -> None:
        if pre_warm:
            self._warm_langkit_metrics()
        self.start()

    def close_process(self) -> None:
        self.close()

    def _get_actor(self) -> "ConfigActor":
        return self

    def is_process_alive(self) -> bool:
        return self.is_alive()
