from faster_fifo import Queue
import logging
from abc import ABC, abstractmethod
from typing import TypeVar, Generic, List, Type
from queue import Full
from tenacity import retry, stop, wait
from multiprocessing import Process
from queue import Empty, Full
from ...util.list_util import get_like_items

MessageType = TypeVar("MessageType")

_DEFAULT_POLL_WAIT_SECONDS = 0.1


class Actor(Process, ABC, Generic[MessageType]):
    def __init__(self, queue: Queue) -> None:
        self.queue = queue
        self._logger = logging.getLogger(type(self).__name__)
        super().__init__()

    @retry(stop=stop.stop_after_attempt(10), wait=wait.wait_fixed(_DEFAULT_POLL_WAIT_SECONDS))
    async def send(self, message: MessageType) -> None:
        done = False
        while not done:
            try:
                self.queue.put(message, timeout=_DEFAULT_POLL_WAIT_SECONDS)
                done = True
            except Full:
                pass

    # TODO is Type the right type here?
    @abstractmethod
    def process_batch(self, batch: List[MessageType], batch_type: Type) -> None:
        pass

    def run(self) -> None:
        while True:
            try:
                messages: List[MessageType] = self.queue.get_many(timeout=0.1, max_messages_to_get=10000)
                (batch, batch_type, next) = get_like_items(messages)
                next_batch = batch
                while next_batch:
                    self._logger.info(f"Processing batch of {len(batch)} {batch_type.__name__}")
                    self.process_batch(batch, batch_type)
                    next_batch = next
            except Empty:
                pass

    def close(self) -> None:
        self.queue.close()
