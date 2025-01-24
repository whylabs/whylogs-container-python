import uuid
from typing import List, Optional, cast

import whylabs_client
from whylabs_client.model.async_log_response import AsyncLogResponse
from whylabs_client.model.segment import Segment

from whylogs.api.logger.experimental.logger.actor.actor import QueueConfig
from whylogs.api.logger.experimental.logger.actor.process_rolling_logger import (
    LoggerOptions,
    ProcessRollingLogger,
    QueueType,
    ThreadLoggerFactory,
    WriterFactory,
)
from whylogs.api.logger.experimental.logger.actor.thread_rolling_logger import ThreadRollingLogger, Writer
from whylogs.api.whylabs.session.whylabs_client_cache import StaticKeyRefresher
from whylogs.api.writer.whylabs import WhyLabsClient, WhyLabsWriter
from whylogs_container.whylabs.container.config import ConfigInstance
from whylogs_container.whylabs.container.server_config import get_server_config
from whylogs_container.whylabs.logger.upload_util import create_presigned_upload_url
from whylogs_container.whylabs.time_utils.time_convert_utils import convert_dataset_cadence, convert_schedule


class LogV1Client(WhyLabsClient):
    def _post_log_async(self, request, dataset_timestamp, zip_file=False):
        server_config = get_server_config()

        print(f"segment tags {request.segment_tags}")

        org_id = cast(str, self._org_id)  # pyright: ignore[reportUnknownMemberType]
        dataset_id = cast(str, self._dataset_id)  # pyright: ignore[reportUnknownMemberType]

        # Example usage
        url = create_presigned_upload_url(
            bucket_name=server_config.s3_profile_upload_bucket,
            org_id=org_id,
            dataset_timestamp=dataset_timestamp,
            prefix=server_config.s3_profile_upload_prefix,
            dataset_id=dataset_id,
            tags=request.segment_tags,
            expiration=3600,
        )

        response = cast(
            AsyncLogResponse,
            AsyncLogResponse(
                tags=Segment(tags=request.segment_tags),
                id=str(uuid.uuid4()),
                model_id=dataset_id,
                upload_timestamp=dataset_timestamp,
                upload_url=url,
            ),
        )

        return response


class MultiOrgWriterFactory(WriterFactory):
    def create_writers(self, dataset_id: str, org_id: Optional[str] = None) -> List[Writer]:
        server_config = get_server_config()

        endpoint = server_config.whylabs_api_endpoint
        configuration = whylabs_client.Configuration(host=endpoint)
        configuration.api_key["ApiKeyAuth"] = server_config.whylabs_api_key  # pyright: ignore[reportUnknownMemberType]
        configuration.discard_unknown_keys = True
        client = whylabs_client.ApiClient(configuration)
        client.set_default_header("X-WhyLabs-Organization", org_id)  # pyright: ignore[reportUnknownMemberType]

        whylogs_client_wrapper = LogV1Client(
            org_id=org_id,
            api_key=server_config.whylabs_api_key,
            dataset_id=dataset_id,
        )

        refresher = StaticKeyRefresher(server_config.whylabs_api_key)
        whylogs_client_wrapper._key_refresher = refresher  # pyright: ignore[reportPrivateUsage]

        return [WhyLabsWriter(whylabs_client=whylogs_client_wrapper)]


class ContainerProcessRollingLogger(ProcessRollingLogger):
    def is_closed(self) -> bool:
        return self._is_closed.is_set()


class ContainerLoggerFactory(ThreadLoggerFactory):
    def __init__(self, config_instance: ConfigInstance) -> None:
        super().__init__()
        self._config_instance = config_instance

    def create_logger(self, dataset_id: str, org_id: Optional[str], options: LoggerOptions) -> ThreadRollingLogger:
        org_id = org_id or get_server_config().org_id
        dataset_config = self._config_instance.config.get_dataset_options(dataset_id, org_id)

        schema = dataset_config.schema
        aggregate_by = convert_dataset_cadence(dataset_config.dataset_cadence)
        write_schedule = convert_schedule(dataset_config.whylabs_upload_cadence.granularity, dataset_config.whylabs_upload_cadence.interval)

        return ThreadRollingLogger(
            aggregate_by=aggregate_by or options.aggregate_by,
            write_schedule=write_schedule or options.write_schedule,
            writers=options.writer_factory.create_writers(dataset_id, org_id),
            schema=schema or options.schema,
            current_time_fn=options.current_time_fn,
            queue_config=options.thread_queue_config,
        )


def create_logger(config_instance: ConfigInstance) -> ContainerProcessRollingLogger:
    config = get_server_config()
    queue_config = QueueConfig(
        max_batch_size=config.max_request_batch_size,
        max_buffer_bytes=config.max_request_buffer_bytes,
    )

    # Setting this makes the logger set up an internal queue that it can use to signal back to the original
    # caller when the message its working on his been processed. This is used to enable sync logging and various
    # sync methods like status(), which gets state out of the container to implement the status/health checks.
    sync_enabled = True

    return ContainerProcessRollingLogger(
        aggregate_by=convert_dataset_cadence(config.default_whylabs_dataset_cadence),
        write_schedule=convert_schedule(config.default_whylabs_upload_cadence, config.default_whylabs_upload_interval),
        logger_factory=ContainerLoggerFactory(config_instance),
        queue_config=queue_config,
        thread_queue_config=queue_config,
        sync_enabled=sync_enabled,
        writer_factory=MultiOrgWriterFactory(),
        queue_type=QueueType.FASTER_FIFO if not config.mac_dev_mode else QueueType.MP,
    )
