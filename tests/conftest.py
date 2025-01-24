import os
from typing import Generator

import pytest


@pytest.fixture(scope="module", autouse=True)
def init_tests() -> Generator[None, None, None]:
    os.environ["WHYLABS_API_KEY"] = "xxxxxxxxxx.xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx:xxx-xxxxxx"
    os.environ["static_secret"] = "password"

    yield

    del os.environ["WHYLABS_API_KEY"]
    del os.environ["static_secret"]
