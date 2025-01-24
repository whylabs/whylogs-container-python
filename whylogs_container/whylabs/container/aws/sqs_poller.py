import logging
import time
import traceback
from threading import Event, Thread
from typing import Any, Dict, Optional

from botocore.exceptions import ClientError
from opentelemetry.trace import get_current_span

from whylogs_container.whylabs.container.aws.client import AWSClientWrapper, get_sqs_consumer_client, get_sqs_sender_client
from whylogs_container.whylabs.container.config import ConfigInstance
from whylogs_container.whylabs.container.dependencies.api_key import WhyLabsApiKey
from whylogs_container.whylabs.container.otel import AttributePrefix
from whylogs_container.whylabs.container.requests import LLMValidateRequest
from whylogs_container.whylabs.container.responses import EvaluationResult
from whylogs_container.whylabs.container.server_config import get_server_config
from whylogs_container.whylabs.llm_validation.validator import SynchronousValidator

logger = logging.getLogger(__name__)


class SQSPoller(Thread):
    def __init__(self, queue_url: str, dlq_url: str, validator: SynchronousValidator, config_instance: ConfigInstance) -> None:
        super().__init__()
        self._queue_url = queue_url
        self._dlq_url = dlq_url
        self._close_event = Event()
        self._validator = validator
        self._config_instance = config_instance
        self._sqs_enqueue_client: Optional[AWSClientWrapper] = None
        self._settings = get_server_config()

    def run(self) -> None:
        wrapper: Optional[AWSClientWrapper] = None
        poll_interval = 10
        logger.info("Starting SQS Poller")

        while not self._close_event.is_set():
            try:
                wrapper = wrapper or get_sqs_consumer_client()

                # Receive message from SQS queue
                response = wrapper.client.receive_message(
                    QueueUrl=self._queue_url,
                    MaxNumberOfMessages=1,
                    WaitTimeSeconds=poll_interval,  # Long polling
                )

                messages = response.get("Messages", [])
                if messages:
                    logger.info(f"Received {len(messages)} messages from sqs")
                    for message in messages:
                        self.process_message(wrapper, message)
                        self.delete_message_from_queue(wrapper, message["ReceiptHandle"])
            except ClientError as e:
                logger.error(f"Failed to poll SQS. Refreshing client credentials: {e}")
                time.sleep(poll_interval)
                wrapper = None
            except Exception as e:
                logger.error(f"Failed to poll SQS: {e}")
                logger.error(traceback.format_exc())
                time.sleep(poll_interval)

    def stop(self) -> None:
        logger.info("Stopping SQS Poller")
        self._close_event.set()
        self.join()

    def process_message(self, wrapper: AWSClientWrapper, message: Dict[str, Any]) -> None:
        body = message["Body"]
        try:
            request = LLMValidateRequest.model_validate_json(body)
        except Exception as e:
            logger.error(f"SQS message can't be parsed. Sending to dlq.: {e}")
            logger.debug("Message body: %s", body)
            self.send_message_to_dlq(wrapper, body, message["ReceiptHandle"])
            return

        span = get_current_span()
        span.set_attribute(AttributePrefix.DatasetId.value, request.dataset_id)

        api_key = WhyLabsApiKey.parse(self._settings.whylabs_api_key)
        try:
            results = self._validator.evaluate(request, lambda: True, log=True, api_key=api_key)
        except Exception as e:
            logger.error(f"Failed to evaluate SQS message for id {request.id}, dataset id {request.dataset_id}. Sending to dlq: {e}")
            self.send_message_to_dlq(wrapper, body, message["ReceiptHandle"])
            return

        # if debug logging enabled, log the results
        if logger.isEnabledFor(logging.DEBUG):
            eval_result = EvaluationResult.from_results(
                results, config=self._config_instance, request=request, perf_info=True, api_key=api_key
            )
            logger.debug(eval_result.model_dump_json(indent=2))

    def send_message_to_dlq(self, wrapper: AWSClientWrapper, body: str, receipt_handle: str) -> None:
        wrapper.client.send_message(QueueUrl=self._dlq_url, MessageBody=body)
        self.delete_message_from_queue(wrapper, receipt_handle)

    def send_message_to_main_queue(self, request: LLMValidateRequest) -> None:
        if self._sqs_enqueue_client is None:
            self._sqs_enqueue_client = get_sqs_sender_client()

        body = request.model_dump_json()
        self._sqs_enqueue_client.client.send_message(QueueUrl=self._queue_url, MessageBody=body)

    def delete_message_from_queue(self, wrapper: AWSClientWrapper, receipt_handle: str) -> None:
        wrapper.client.delete_message(QueueUrl=self._queue_url, ReceiptHandle=receipt_handle)
