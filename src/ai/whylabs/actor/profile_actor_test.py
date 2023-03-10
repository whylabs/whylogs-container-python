from faster_fifo import Queue
import base64
from multiprocessing import Value
from ctypes import c_int

from whylogs.api.writer.writer import Writable
import logging
from typing import Generator, Tuple, Optional, Any
from whylogs.api.writer import Writer
import pytest
import os

# TODO stop config from evaluating as a side effect of importing config
os.environ["WHYLABS_API_KEY"] = "xxxx"
os.environ["WHYLABS_ORG_ID"] = "org-90"
os.environ["CONTAINER_PASSWORD"] = "password"
os.environ["DEFAULT_WHYLABS_DATASET_CADENCE"] = "HOURLY"

from ..actor.profile_actor import (
    DebugMessage,
    ProfileActor,
    PublishMessage,
    RawLogMessage,
    RawLogEmbeddingsMessage,
    RawPubSubMessage,
)
from ..actor.actor import start_actor
from ..container.requests import LogRequest, LogMultiple, PubSubRequest, PubSubMessage

logging.basicConfig(handlers=[])
# logging.root.setLevel(logging.DEBUG)

actor: ProfileActor
writer: Writer


def pubsub_message(request: LogRequest) -> PubSubRequest:
    return PubSubRequest(
        subscription="_",
        message=PubSubMessage(
            attributes={}, message_id=123, publish_time=1, data=base64.b64encode(request.json().encode("utf-8"))
        ),
    )


class DummyWriter(Writer):
    def __init__(self) -> None:
        self.write_count: Any = Value(c_int, 0)
        super().__init__()

    def write(self, file: Writable, dest: Optional[str] = None, **kwargs: Any) -> Tuple[bool, str]:
        with self.write_count.get_lock():
            self.write_count.value += 1

        return True, ""

    def option(self, **kwargs: Any) -> "DummyWriter":
        return self


@pytest.fixture(autouse=True)
def run_around_tests() -> Generator[None, None, None]:
    global actor
    global writer
    writer = DummyWriter()
    actor = ProfileActor(Queue(1000), writers=[writer])
    start_actor(actor)
    yield
    if not actor._work_done_signal.is_set():
        actor.shutdown()


def test_log_tabular() -> None:
    global actor
    log_message = LogRequest(
        dataset_id="1", timestamp=1, multiple=LogMultiple(columns=["col1", "col2"], data=[[1, "a"], [2, "b"]])
    )
    actor.send(RawLogMessage(request=bytes(log_message.json(), "utf-8"), request_time=2))
    actor.shutdown()
    assert writer.write_count.value == 1


def test_log_tabular_multiple_datasets() -> None:
    global actor
    # Log three messages to three different datasets
    data = LogMultiple(columns=["col1", "col2"], data=[[1, "a"], [2, "b"]])
    log_message = LogRequest(dataset_id="1", timestamp=1, multiple=data)
    log_message2 = LogRequest(dataset_id="2", timestamp=1, multiple=data)
    log_message3 = LogRequest(dataset_id="3", timestamp=1, multiple=data)
    actor.send(RawLogMessage(request=bytes(log_message.json(), "utf-8"), request_time=2))
    actor.send(RawLogMessage(request=bytes(log_message2.json(), "utf-8"), request_time=2))
    actor.send(RawLogMessage(request=bytes(log_message3.json(), "utf-8"), request_time=2))
    actor.shutdown()
    assert writer.write_count.value == 3


def test_log_tabular_multiple_timestamps() -> None:
    global actor
    # Log three requests to the same dataset one hour part each
    data = LogMultiple(columns=["col1", "col2"], data=[[1, "a"], [2, "b"]])
    log_message = LogRequest(dataset_id="1", timestamp=1678659050000, multiple=data)
    log_message2 = LogRequest(dataset_id="1", timestamp=1678651850000, multiple=data)
    log_message3 = LogRequest(dataset_id="1", timestamp=1678648250000, multiple=data)
    actor.send(RawLogMessage(request=bytes(log_message.json(), "utf-8"), request_time=1))
    actor.send(RawLogMessage(request=bytes(log_message2.json(), "utf-8"), request_time=1))
    actor.send(RawLogMessage(request=bytes(log_message3.json(), "utf-8"), request_time=1))
    actor.shutdown()
    assert writer.write_count.value == 3


def test_pubsub_message() -> None:
    global actor
    data = LogMultiple(columns=["col1", "col2"], data=[[1, "a"], [2, "b"]])
    log_request = LogRequest(dataset_id="1", timestamp=1, multiple=data)
    request = pubsub_message(log_request)
    request2 = pubsub_message(log_request)
    request3 = pubsub_message(log_request)

    actor.send(RawPubSubMessage(bytes(request.json(), "utf-8"), request_time=2))
    actor.send(RawPubSubMessage(bytes(request2.json(), "utf-8"), request_time=2))
    actor.send(RawPubSubMessage(bytes(request3.json(), "utf-8"), request_time=2))
    actor.shutdown()
    assert writer.write_count.value == 1


def test_pubsub_message_different_datasets() -> None:
    global actor
    # Log three messages to three different datasets
    data = LogMultiple(columns=["col1", "col2"], data=[[1, "a"], [2, "b"]])
    request = pubsub_message(LogRequest(dataset_id="1", timestamp=1, multiple=data))
    request2 = pubsub_message(LogRequest(dataset_id="2", timestamp=1, multiple=data))
    request3 = pubsub_message(LogRequest(dataset_id="3", timestamp=1, multiple=data))

    actor.send(RawPubSubMessage(bytes(request.json(), "utf-8"), request_time=2))
    actor.send(RawPubSubMessage(bytes(request2.json(), "utf-8"), request_time=2))
    actor.send(RawPubSubMessage(bytes(request3.json(), "utf-8"), request_time=2))
    actor.shutdown()
    assert writer.write_count.value == 3


def test_pubsub_message_different_timestamps() -> None:
    global actor
    data = LogMultiple(columns=["col1", "col2"], data=[[1, "a"], [2, "b"]])
    # Log three messages to three different hour timestamps
    request = pubsub_message(LogRequest(dataset_id="1", timestamp=1678659050000, multiple=data))
    request2 = pubsub_message(LogRequest(dataset_id="1", timestamp=1678651850000, multiple=data))
    request3 = pubsub_message(LogRequest(dataset_id="1", timestamp=1678648250000, multiple=data))

    actor.send(RawPubSubMessage(bytes(request.json(), "utf-8"), request_time=2))
    actor.send(RawPubSubMessage(bytes(request2.json(), "utf-8"), request_time=2))
    actor.send(RawPubSubMessage(bytes(request3.json(), "utf-8"), request_time=2))
    actor.shutdown()
    assert writer.write_count.value == 3
