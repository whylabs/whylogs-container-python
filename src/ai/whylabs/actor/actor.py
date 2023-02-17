from faster_fifo import Queue
import os
import asyncio
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
    async def process_batch(self, batch: List[MessageType], batch_type: Type) -> None:
        pass

    async def process_messages(self) -> None:
        while not self._work_done_signal.is_set():
            try:
                try:
                    messages: List[MessageType] = self.queue.get_many(timeout=0.1, max_messages_to_get=10000)
                    for (batch, batch_type) in type_batched_items(messages):
                        self._logger.info(f"Processing batch of {len(batch)} {batch_type.__name__}")
                        await self.process_batch(batch, batch_type)

                except Empty:
                    if self.queue.is_closed():
                        self._logger.info(f"Queue closed and no more messages to process.")
                        self._work_done_signal.set()
            except KeyboardInterrupt:
                self._logger.info(f"Shutting down actor.")
            except BaseException as e:
                # Catches KeyboardInterrupt as well, which Exception doesn't
                self._logger.exception(e)

    def run(self) -> None:
        self._loop = asyncio.get_event_loop()
        self._loop.run_until_complete(self.process_messages())
        # self.process_messages()

    async def shutdown(self) -> None:
        self._logger.info("Sending Close message to work queue.")
        await self.send(CloseMessage())
        self._work_done_signal.wait()
        os._exit(0)  # Not sure why I need this but I definitely do
