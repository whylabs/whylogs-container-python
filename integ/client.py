import time
from typing import Callable, TypeVar

import whylogs_container_client.api.manage.health as Health
import whylogs_container_client.api.manage.status as Status
import whylogs_container_client.api.profile.log as Log
from whylogs_container_client import AuthenticatedClient
from whylogs_container_client.models import LogRequest
from whylogs_container_client.models.status_response import StatusResponse

T = TypeVar("T")


def _retry(func: Callable[[], T], max_retries=10, interval=0.5) -> T:
    """
    Retry a function until it succeeds or the max_retries is reached.
    """
    retry_count = 0
    while retry_count < max_retries:
        try:
            return func()
        except Exception:
            time.sleep(interval)
            retry_count += 1

    raise Exception(f"Failed to run function after {retry_count} retries")


class ContainerClient:
    def __init__(self, port: int = 8000) -> None:
        self.port = port
        self.client = AuthenticatedClient(base_url=f"http://localhost:{port}", token="password", prefix="", auth_header_name="X-API-Key")

    def health(self) -> None:
        """
        Check if the container is running. Raises an exception if the container is not running after retries are done.
        """
        # _retry(self._check_health, max_retries=20, interval=2)
        _retry(lambda: Health.sync_detailed(client=self.client), max_retries=20)

    def status(self) -> StatusResponse:
        """
        Get the current status of the container.
        """
        response = Status.sync_detailed(client=self.client)

        if not response.parsed:
            raise Exception(f"Failed to get status. Status code: {response.status_code}")

        return response.parsed

    def log(self, data: LogRequest) -> None:
        """
        Log a message to the container.
        """
        response = Log.sync_detailed(client=self.client, body=data)

        if response.status_code != 200:
            raise Exception(f"Failed to log data. Status code: {response.status_code}")
