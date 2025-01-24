import pytest

from whylogs_container.whylabs.container.dependencies.api_key_util import parse_api_key


def test_api_key_parses():
    fake_real_key = "xxxx.xxxxxxxxxxxx:org-123"
    parsed_key = parse_api_key(fake_real_key)

    assert parsed_key.org_id == "org-123"
    assert parsed_key.id == "xxxx"


def test_fake_key_fails():
    fake_key = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx/xxxxxxxxxxx"

    with pytest.raises(ValueError):
        parse_api_key(fake_key)
