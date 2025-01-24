import asyncio
import logging
import os
import threading
import uuid
from abc import ABC, abstractmethod
from concurrent.futures import Future
from dataclasses import dataclass, field
from queue import Empty, Queue
from typing import Any, Callable, Coroutine, Dict, List, Literal, Optional, Union, cast

import orjson

from langkit.core.workflow import WorkflowResult
from whylogs.api.logger.experimental.logger.actor.actor import QueueConfig
from whylogs.api.logger.experimental.logger.actor.future_util import wait_result_while
from whylogs.api.logger.experimental.logger.actor.pipe_signaler import PipeSignaler
from whylogs.api.logger.experimental.logger.actor.process_actor import ProcessActor
from whylogs.api.logger.experimental.logger.actor.process_rolling_logger import LogRequestDict, QueueType
from whylogs.api.logger.experimental.logger.actor.process_rolling_logger_messages import DataDict, RawLogMessage
from whylogs.api.logger.experimental.logger.actor.time_util import current_time_ms
from whylogs_container.whylabs.container.config import ConfigInstance
from whylogs_container.whylabs.container.dependencies.api_key import WhyLabsApiKey
from whylogs_container.whylabs.container.otel import trace_langkit_result
from whylogs_container.whylabs.container.requests import LLMValidateRequest
from whylogs_container.whylabs.container.server_config import get_server_config
from whylogs_container.whylabs.logger.logger import ContainerProcessRollingLogger

_logger = logging.getLogger(__name__)

ValidationTarget = Union[Literal["prompt"], Literal["response"]]


@dataclass
class ReturnType:
    result: WorkflowResult
    score_result: Optional[WorkflowResult]
    wf_metadata: Optional[Dict[str, Dict[str, Any]]] = None


@dataclass
class ValidationMessage:
    request: bytes
    timestamp: int
    api_key: WhyLabsApiKey
    log: bool = True
    trace: bool = True
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    carrier: Optional[Dict[str, Any]] = None

    def to_validation_request(self) -> LLMValidateRequest:
        return LLMValidateRequest.model_validate_json(self.request)


@dataclass
class AsyncValidationMessage(ValidationMessage):
    pass


@dataclass
class QueueLockEvents:
    work_event: threading.Event
    enqueue_event: threading.Event


class QueueLock(threading.Thread):
    def __init__(self) -> None:
        super().__init__()
        self.queue: Queue[QueueLockEvents] = Queue()
        self._closed = threading.Event()

    def request(self, work_event: threading.Event) -> threading.Event:
        enqueue_event = threading.Event()
        self.queue.put(QueueLockEvents(work_event=work_event, enqueue_event=enqueue_event))
        return enqueue_event

    def run(self) -> None:
        while not self._closed.is_set():
            try:
                events = self.queue.get(timeout=1)
                _logger.debug("Poping request from validation queue. %s requests left", self.queue.qsize())
                events.enqueue_event.set()
                events.work_event.wait(timeout=20)
            except Empty:
                pass
            except Exception as e:
                _logger.exception("Error in QueueLock", e)

    def close(self) -> None:
        self._closed.set()


CallableOrCoroutinePredicate = Union[Callable[[], bool], Coroutine[None, None, bool]]


class SynchronousValidator(ABC):
    """
    This class is a convenience wrapper that has all of the logic required for synchronously
    calling the ValidationActor. There shouldn't be any real logic in here that does validation,
    just calls to actor.send and synchronous utilities.
    """

    def evaluate(
        self,
        request: LLMValidateRequest,
        active_predicate: CallableOrCoroutinePredicate,
        api_key: WhyLabsApiKey,
        log: bool = True,
        trace: bool = True,
        carrier: Optional[Dict[str, Any]] = None,
    ) -> ReturnType:
        """
        Perform validation on the given LLM message and wait for the result.
        """

        work = threading.Event()
        try:
            self._get_queue_lock().request(work).wait()

            if callable(active_predicate):
                active = active_predicate()
            else:
                active = asyncio.run_coroutine_threadsafe(active_predicate, self._get_event_loop()).result()

            if not active:
                raise TimeoutError("Original request no longer active")

            body: bytes = request.model_dump_json(by_alias=True).encode("utf-8")

            message = ValidationMessage(
                request=body, log=log, timestamp=request.timestamp, trace=trace, carrier=carrier if trace else None, api_key=api_key
            )
            future: Future[Optional[ReturnType]] = Future()
            self._get_pipe_signaler().register(future, message.id)
            self._get_actor().send(message)

            result = wait_result_while(future, self._is_alive)

            if result is None:
                raise Exception("Unexpected None result from validation actor")

            return result
        except TimeoutError as e:
            _logger.warning("Validation request timed out because client is no longer active")
            raise e
        except Exception as e:
            _logger.exception("Error in validation workflow", e)
            raise e
        finally:
            work.set()

    def warm_caches(self) -> None:
        self.evaluate(
            LLMValidateRequest(datasetId="default", prompt="test", response="test"),
            lambda: True,
            log=False,
            trace=False,
            api_key=WhyLabsApiKey(api_key="", org_id="default", id=""),
        )

    @abstractmethod
    def _get_queue_lock(self) -> QueueLock:
        raise NotImplementedError()

    @abstractmethod
    def _get_event_loop(self) -> asyncio.AbstractEventLoop:
        raise NotImplementedError()

    @abstractmethod
    def _get_actor(self) -> "ValidationActor":
        raise NotImplementedError()

    @abstractmethod
    def _get_rolling_logger(self) -> Optional[ContainerProcessRollingLogger]:
        raise NotImplementedError()

    @abstractmethod
    def start_validator(self) -> None:
        pass

    @abstractmethod
    def close_validator(self) -> None:
        pass

    @abstractmethod
    def _get_pipe_signaler(self) -> PipeSignaler[Optional[ReturnType]]:
        raise NotImplementedError()

    @abstractmethod
    def _is_alive(self) -> bool:
        """
        Should be a pass through to Process.is_alive()
        """
        raise NotImplementedError()


Messages = Union[ValidationMessage, AsyncValidationMessage]


class ValidationActor(ProcessActor[Messages, Optional[ReturnType]], SynchronousValidator):
    def __init__(
        self,
        config_instance: ConfigInstance,
        rolling_logger: Optional[ContainerProcessRollingLogger],
        event_loop: asyncio.AbstractEventLoop,
    ) -> None:
        queue_config = QueueConfig(
            max_batch_size=1, message_accumualtion_duration=0.001, message_poll_wait=0.005, max_buffer_bytes=100_000_000
        )
        settings = get_server_config()
        super().__init__(
            sync_enabled=True,
            queue_config=queue_config,
            queue_type=QueueType.FASTER_FIFO if not settings.mac_dev_mode else QueueType.MP,
        )

        self._config_instance = config_instance
        self._rolling_logger = rolling_logger
        self._queue_lock = QueueLock()
        # We take this in as a parameter so that we can implement the SynchronousValidator interface but don't actually
        # use this from the forked process, that wouldn't work. It's only used in the parent process as part of utility
        # functions that make calling this actor easier.
        self._event_loop = event_loop

    def _get_actor(self) -> "ValidationActor":
        return self

    def _get_rolling_logger(self) -> Optional[ContainerProcessRollingLogger]:
        return self._rolling_logger

    def _get_pipe_signaler(self) -> PipeSignaler[Optional[ReturnType]]:
        if not self._pipe_signaler:
            raise Exception("PipeSignaler not initialized")

        return self._pipe_signaler

    def _is_alive(self) -> bool:
        return super().is_alive()

    def start_validator(self) -> None:
        self._queue_lock.start()
        self.start()

    def run(self) -> None:
        return super().run()

    def close_validator(self) -> None:
        self._queue_lock.close()
        self.close()

    def process_batch(self, batch, batch_type) -> None:
        if batch_type == AsyncValidationMessage:
            self.process_async_validation_message(cast(List[AsyncValidationMessage], batch))
        elif batch_type == ValidationMessage:
            self.process_validation_message(cast(List[ValidationMessage], batch))

    def _log_request(
        self, request: LLMValidateRequest, results: WorkflowResult, score_result: Optional[WorkflowResult], message: ValidationMessage
    ) -> None:
        split = results.metrics.to_dict(orient="split")  # pyright: ignore[reportUnknownVariableType, reportUnknownMemberType]
        result_cols: List[str] = list(split["columns"])  # pyright: ignore[reportUnknownArgumentType]
        result_data = split["data"][0]  # pyright: ignore[reportUnknownVariableType]

        score_split = score_result.metrics if score_result else None
        if score_split is not None:
            # delete the id column, its already added in the non-score metric workflow
            score_split.drop(columns=["id"], inplace=True)
            score_dict = score_split.to_dict(orient="split")  # pyright: ignore[reportUnknownVariableType, reportUnknownMemberType]
            score_cols: List[str] = list(score_dict["columns"])  # pyright: ignore[reportUnknownArgumentType]
            score_data = score_dict["data"][0]  # pyright: ignore[reportUnknownVariableType]
        else:
            score_cols = []
            score_data = []

        data_dict = request.to_data_dict()
        data_dict["columns"] += result_cols
        data_dict["columns"] += score_cols
        data_dict["data"][0] += result_data
        data_dict["data"][0] += score_data

        log_request = LogRequestDict(
            datasetId=request.dataset_id,
            timestamp=message.timestamp,
            multiple=DataDict(columns=data_dict["columns"], data=data_dict["data"]),
            orgId=message.api_key.org_id,
        )
        request_bytes = orjson.dumps(log_request)
        if self._rolling_logger:
            self._rolling_logger.send(RawLogMessage(request=request_bytes, request_time=current_time_ms()))

    def process_validation_message(self, messages: List[ValidationMessage]) -> None:
        self.process_validation_messages(messages, return_results=True)

    def process_async_validation_message(self, messages: List[AsyncValidationMessage]) -> None:
        self.process_validation_messages(messages, return_results=False)

    def process_validation_messages(
        self, messages: Union[List[AsyncValidationMessage], List[ValidationMessage]], return_results: bool
    ) -> None:
        for message in messages:
            try:
                request = message.to_validation_request()
            except Exception as e:
                _logger.exception(f"Error parsing request {message.request}", e)
                self._get_pipe_signaler().signal((message.id, e, None))
                continue

            try:
                wf = self._config_instance.config.get_langkit_workflow(dataset_id=request.dataset_id, org_id=message.api_key.org_id)
                results = wf.run(request.to_dataframe(), options=request.options)
                metadata = wf.get_metric_metadata()

                score_wf = self._config_instance.config.get_langkit_score_workflow(
                    dataset_id=request.dataset_id, org_id=message.api_key.org_id
                )

                if score_wf:
                    score_results = score_wf.run(results.metrics)
                else:
                    score_results = None

                if message.trace:
                    trace_langkit_result(
                        result=results,
                        score_result=score_results,
                        config=self._config_instance,
                        dataset_id=request.dataset_id,
                        start_time_ns=request.timestamp * 1_000_000,
                        end_time_ns=current_time_ms() * 1_000_000,
                        carrier=message.carrier or {},
                        metric_metadata=metadata,
                        additional_data=request.additional_data,
                        api_key=message.api_key,
                        request_metadata=request.metadata,
                    )

                if message.log:
                    self._log_request(request, results, score_results, message)

                if return_results:
                    self._get_pipe_signaler().signal(
                        (
                            message.id,
                            None,
                            ReturnType(result=results, score_result=score_results, wf_metadata=metadata),
                        )
                    )
                else:
                    self._get_pipe_signaler().signal((message.id, None, None))
            except Exception as e:
                _logger.exception(
                    "Error in validation workflow while processing request for dataset "
                    f"id {request.dataset_id}, in org {message.api_key.org_id}",
                    e,
                )
                self._get_pipe_signaler().signal((message.id, e, None))

    def _get_queue_lock(self) -> QueueLock:
        return self._queue_lock

    def _get_event_loop(self) -> asyncio.AbstractEventLoop:
        return self._event_loop

    def start(self) -> None:
        self._logger.debug(f"Starting validator from pid {os.getpid()}")
        super().start()
