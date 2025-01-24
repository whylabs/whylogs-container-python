import os
import random
import shutil
import signal
import subprocess
from dataclasses import dataclass
from typing import Any, Callable, Dict, Generator, List, NamedTuple, Optional

import pytest

from integ.client import ContainerClient


class ServerCommands:
    def local(self, port: str) -> List[str]:
        return [
            "python",
            "-m",
            "whylogs_container.whylabs.container.startup",
            str(port),
        ]

    def docker(self, port: str) -> List[str]:
        return [
            "docker",
            "run",
            "-p",
            f"127.0.0.1:{port}:{port}",
            "--env",
            "WHYLABS_API_KEY=test",
            "--env",
            "DEFAULT_MODEL_ID=model-62",
            "--env",
            "static_secret=password",
            "--env",
            "DEFAULT_WHYLABS_DATASET_CADENCE=DAILY",
            "--env",
            "DEFAULT_WHYLABS_UPLOAD_CADENCE=M",
            "--env",
            "DEFAULT_WHYLABS_UPLOAD_INTERVAL=5",
            "--env",
            "FAIL_STARTUP_WITHOUT_CONFIG=False",
            "whylabs/whylogs:py-llm-latest",
        ]


@dataclass(frozen=True)
class Server:
    port: int
    client: ContainerClient


OnDemandServerFixture = Callable[[], Server]


def create_server(port: int) -> subprocess.Popen[bytes]:
    if "TEST_USE_DOCKER" in os.environ:
        proc = subprocess.Popen(ServerCommands().docker(str(port)))
    else:
        env: Dict[str, str] = os.environ.copy()
        env.update(
            {
                # "WHYLABS_API_KEY": "", # set in CI or locally in shell
                "DEFAULT_MODEL_ID": "model-62",
                "STATIC_SECRET": "password",
                "DEFAULT_WHYLABS_DATASET_CADENCE": "DAILY",
                "DEFAULT_WHYLABS_UPLOAD_CADENCE": "M",
                "DEFAULT_WHYLABS_UPLOAD_INTERVAL": "5",
                "FAIL_STARTUP_WITHOUT_CONFIG": "False",
            }
        )
        proc = subprocess.Popen(ServerCommands().local(str(port)), env=env)

    return proc


@pytest.fixture(scope="module")
def on_demand_server() -> Generator[Callable[[], Server], None, None]:
    print("module init")

    port = random.randint(10000, 11000)
    client = ContainerClient(port)

    proc: Optional[subprocess.Popen[bytes]] = None

    def start():
        nonlocal proc
        proc = create_server(port=port)
        client.health()
        return Server(port, client)

    yield start

    if proc is None:
        raise Exception("Server is not started")

    proc.send_signal(signal.SIGINT)
    proc.wait()


@pytest.fixture(scope="module")
def server() -> Generator[Server, None, None]:
    print("module init")

    port = random.randint(8000, 9000)
    client = ContainerClient(port)

    proc = create_server(port=port)
    client.health()
    yield Server(port, client)
    proc.send_signal(signal.SIGINT)
    proc.wait()


class ConfigFile(NamedTuple):
    suffix: str
    config_file: str


class ConfigFiles(NamedTuple):
    files: List[ConfigFile]
    dir_name: str = "tmp_dir"


def config_file_fixture(
    config_files: ConfigFiles,
) -> Callable[[], Generator[None, Any, None]]:
    """
    This is a broadly scoped fixture that will create a temporary directory and write strings as files to it.
    It is lazily evaluated, so that users will define the strings they want to write to the file in the test,
    specify the suffix and then write files for specific tests.
    """

    @pytest.fixture(scope="function")
    def custom_file_config() -> Generator[None, Any, None]:
        os.makedirs(name=config_files.dir_name, exist_ok=True)
        os.environ["LLM_CONTAINER"] = "true"
        os.environ["POLICY_BASE_DIR"] = config_files.dir_name

        for n, config_file in enumerate(config_files.files):
            file_path = os.path.join(config_files.dir_name, f"temp_file_{n}{config_file.suffix}")
            with open(file_path, mode="w+") as tmp_file:
                tmp_file.write(config_file.config_file)
        yield

        del os.environ["POLICY_BASE_DIR"]
        del os.environ["LLM_CONTAINER"]
        shutil.rmtree(config_files.dir_name)

    return custom_file_config
