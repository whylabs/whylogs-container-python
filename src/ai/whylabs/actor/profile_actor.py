import io
import logging
from dataclasses import dataclass
from typing import Dict, List, Type, Union, cast

import pandas as pd
from faster_fifo import Queue
from whylogs.core import DatasetProfile

from .actor import Actor


@dataclass
class CSVMessage:
    csv: bytes


class DebugMessage:
    pass


MessageType = Union[CSVMessage, DebugMessage]


class ProfileActor(Actor[MessageType]):
    def __init__(self, queue: Queue) -> None:
        super().__init__(queue)
        # TODO next thing to figure out is how I should be using why.log to work with result sets instead of directly with DatasetProfile
        self.profiles: Dict[str, DatasetProfile] = {"default": DatasetProfile()}

    def process_batch(self, batch: List[MessageType], batch_type: Type) -> None:
        if batch_type == CSVMessage:
            self.process_csv(cast(List[CSVMessage], batch))
        elif batch_type == DebugMessage:
            self.process_debug_message(cast(List[DebugMessage], batch))
        elif batch_type == type(None):
            pass
        else:
            raise Exception(f"Unknown message type {batch_type}")

    def process_csv(self, messages: List[CSVMessage]) -> None:
        self._logger.info("Processing csv message")
        csv_list = map(lambda message: message.csv, messages)
        dfs = [pd.read_csv(io.BytesIO(csv_bytes), engine="pyarrow") for csv_bytes in csv_list]
        df = pd.concat(dfs)

        profile = self.profiles["default"]
        profile.track(df)

    def process_debug_message(self, messages: List[DebugMessage]) -> None:
        self._logger.debug(self.profiles["default"].view().to_pandas())
