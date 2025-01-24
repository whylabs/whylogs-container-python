import os
from dataclasses import dataclass

import pytest

from whylogs_container.whylabs.container.server_config import reset_server_config
from whylogs_container.whylabs.container.version import version


@dataclass
class SettingsParams:
    tracing_enabled: bool
    llm_container: bool


cases = [
    SettingsParams(tracing_enabled=True, llm_container=True),
    SettingsParams(tracing_enabled=False, llm_container=True),
    SettingsParams(tracing_enabled=True, llm_container=False),
    SettingsParams(tracing_enabled=False, llm_container=False),
]


@pytest.fixture(scope="function", params=cases)
def params(request: pytest.FixtureRequest):
    os.environ["disable_auth"] = "True"
    os.environ["static_secret"] = "password"

    os.environ["LLM_CONTAINER"] = "true" if request.param.llm_container else "false"
    os.environ["DISABLE_TRACING"] = "false" if request.param.tracing_enabled else "true"

    reset_server_config()

    yield request.param

    reset_server_config()


def test_container_headers(params: SettingsParams):
    from whylogs_container.whylabs.container.responses import get_container_headers

    headers = get_container_headers()

    if not params.llm_container:
        assert headers == {}
        return

    if params.tracing_enabled:
        assert headers == {
            "x-wls-verconstr": ">=0.0.1",
            "x-wls-version": version,
            "x-wls-sst": "1",
        }
    else:
        assert headers == {
            "x-wls-verconstr": ">=0.0.1",
            "x-wls-version": version,
            "x-wls-sst": "0",
        }
