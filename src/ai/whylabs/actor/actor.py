from faster_fifo import Queue
import signal
from ...util.signal_util import suspended_signals
from typing import Optional
import time
import os
import logging
from abc import ABC, abstractmethod
from typing import TypeVar, Generic, List, Type, Union
from queue import Full
from multiprocessing import Process, Event
from queue import Empty, Full
from ...util.list_util import get_like_items, type_batched_items

MessageType = TypeVar("MessageType")

_DEFAULT_POLL_WAIT_SECONDS = 0.1


class CloseMessage:
    pass


class Actor(Process, ABC, Generic[MessageType]):
    def __init__(self, queue: Queue) -> None:
        self.queue = queue
        self._logger = logging.getLogger(f"{type(self).__name__}_{id(self)}")
        self._work_done_signal = Event()
        super().__init__()

    async def send(self, message: Union[CloseMessage, MessageType]) -> None:
        if self.queue.is_closed():
            self._logger.warn(f"Dropping message because queue is closed.")
            return

        if isinstance(message, CloseMessage):
            self.queue.close()

        done = False
        while not done:
            try:
                self.queue.put(message, timeout=_DEFAULT_POLL_WAIT_SECONDS)
                done = True
            except Full:
                self._logger.warn(f"Message queue full, trying again")

    @abstractmethod
    def process_batch(self, batch: List[MessageType], batch_type: Type) -> None:
        pass

    def _polling_condition(self, batch_len: int, max: int, last_message_time: float, remaining: int) -> bool:
        if self._work_done_signal.is_set() and remaining == 0:
            self._logger.info("Stopping poll. Handled all messages and shutting down.")
            return False

        if batch_len > max:
            self._logger.info(f"Stopping poll. Got {max} messages.")
            return False

        if time.perf_counter() - last_message_time > 0.5:
            # self._logger.debug("Stopping poll. Went max time without seeing new messages.")
            return False

        # self._logger.debug(f'Continuing poll. Have {batch_len} messages and {remaining} left.')
        return True

    def _load_messages(self) -> Optional[List[MessageType]]:
        max = 10000
        batch: List[MessageType] = []
        last_message_time = time.perf_counter()

        while self._polling_condition(len(batch), max, last_message_time, self.queue.qsize()):
            try:
                batch += self.queue.get_many(timeout=0.1, max_messages_to_get=max)
                last_message_time = time.perf_counter()
            except Empty:
                if self.queue.is_closed():
                    self._logger.info(f"Queue closed and no more messages to process.")
                    return None if batch == [] else batch

        return batch

    def process_messages(self) -> None:
        messages: Optional[List[MessageType]] = []
        while messages is not None:
            messages = self._load_messages()

            if messages is None:
                continue

            for (batch, batch_type) in type_batched_items(messages):
                if batch == []:
                    continue

                self._logger.info(
                    f"Processing batch of {len(batch)} {batch_type.__name__}. {self.queue.qsize()} remaining"
                )
                self.process_batch(batch, batch_type)

        # Can only get here if we're done processing messages
        self._work_done_signal.set()

    def run(self) -> None:
        try:
            with suspended_signals(signal.SIGINT, signal.SIGTERM):
                self.process_messages()
        except KeyboardInterrupt:
            pass
        except BaseException as e:
            self._logger.exception(e)

    async def shutdown(self) -> None:
        self._logger.info("Sending Close message to work queue.")
        await self.send(CloseMessage())
        self._logger.info(
            f"Process will shutdown after all pending {self.queue.qsize()} data has been processed and uploaded."
        )
        self._work_done_signal.wait()
        self._logger.info("Process shutting down.")
        os._exit(0)  # Not sure why I need this but I definitely do
