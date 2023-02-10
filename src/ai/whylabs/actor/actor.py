from faster_fifo import Queue
import time
import asyncio
import logging
from abc import ABC, abstractmethod
from typing import TypeVar, Generic, List, Type, Union
from queue import Full
from multiprocessing import Process, Event
from queue import Empty, Full
from ...util.list_util import get_like_items

MessageType = TypeVar("MessageType")

_DEFAULT_POLL_WAIT_SECONDS = 0.1

# Callback on the rolling logger for debug info

class CloseMessage:
    pass


class Actor(Process, ABC, Generic[MessageType]):
    def __init__(self, queue: Queue) -> None:
        self.queue = queue
        self._logger = logging.getLogger(type(self).__name__)
        self._stop_signal = Event()
        self._work_done_signal = Event()
        super().__init__()

    # TODO some type issue here
    # @retry(stop=stop.stop_after_attempt(10), wait=wait.wait_fixed(_DEFAULT_POLL_WAIT_SECONDS))
    async def send(self, message: Union[CloseMessage, MessageType]) -> None:
        done = False
        while not done:
            try:
                self.queue.put(message, timeout=_DEFAULT_POLL_WAIT_SECONDS)
                done = True
            except Full:
                self._logger.warn(f'Message queue full, trying again')
                pass

    # TODO is Type the right type here?
    @abstractmethod
    async def process_batch(self, batch: List[MessageType], batch_type: Type) -> None:
        pass

    async def process_messages(self) -> None:
        while not self._stop_signal.is_set():
            try:
                try:
                    # if self.queue.qsize() < 1000:
                    #     await asyncio.sleep(1)
                    #     continue
                    messages: List[MessageType] = self.queue.get_many(timeout=0.1, max_messages_to_get=10000)
                    (batch, batch_type, next) = get_like_items(messages)
                    next_batch = batch
                    while next_batch:
                        self._logger.info(f"Processing batch of {len(batch)} {batch_type.__name__}")
                        await self.process_batch(batch, batch_type)
                        next_batch = next
                except Empty:
                    pass
            except BaseException:
                # Catches KeyboardInterrupt as well, which Exception doesn't
                pass

        
        self._logger.info(f'Message processing done, sending done signal')
        self._work_done_signal.set()

    def run(self) -> None:
        self._loop = asyncio.get_event_loop()
        self._loop.run_until_complete(self.process_messages())
        # self.process_messages()

    async def shutdown(self) -> None:
        await self.send(CloseMessage())
        self.queue.close()
        self._stop_signal.set()
        self._work_done_signal.wait()
