from datetime import datetime

from dateutil import tz
from whylogs_container_client.models.log_multiple import LogMultiple
from whylogs_container_client.models.log_request import LogRequest

from integ.assert_util import assert_profile, get_profile_list
from integ.conftest import Server
from whylogs.core.dataset_profile import DatasetProfile


def test_log(server: Server):
    time_ms = 1701284201000  # Wednesday, November 29, 2023 6:56:41 PM
    truncated_time_ms = 1701216000000

    request = LogRequest(
        dataset_id="model-1",
        timestamp=time_ms,
        multiple=LogMultiple(
            columns=["col1", "col2"],
            data=[[1, 2], [3, 4]],
        ),
    )

    server.client.log(request)
    profiles = get_profile_list(server.client.status())

    assert len(profiles) == 1

    profile = profiles[0]

    expected = DatasetProfile(dataset_timestamp=datetime.fromtimestamp(truncated_time_ms / 1000.0, tz=tz.tzutc()))
    expected.track({"col1": 1, "col2": 2})  # type: ignore[reportUnknownMemberType]
    expected.track({"col1": 3, "col2": 4})  # type: ignore[reportUnknownMemberType]

    assert_profile(profile, expected.view())
