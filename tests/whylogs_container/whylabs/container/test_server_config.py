import os
import tempfile
import uuid
from typing import IO

import pytest

from whylogs_container.whylabs.container.server_config import SecretsFile, ServerConfig


@pytest.fixture(scope="function")
def _secrets_file_fixture():  # pyright: ignore[reportUnusedFunction]
    with tempfile.NamedTemporaryFile() as f:
        orig_secrets_json_path = os.environ.get("secrets_path")
        os.environ["secrets_path_json"] = f.name

        orig_secrets_path_files = os.environ.get("secrets_path_files")
        os.environ["secrets_path_files"] = os.path.dirname(f.name)

        orig_whylabs_api_key = os.environ.get("WHYLABS_API_KEY")
        del os.environ["WHYLABS_API_KEY"]

        orig_container_password = os.environ.get("static_secret")
        del os.environ["static_secret"]

        yield f

    if orig_secrets_json_path is not None:
        os.environ["secrets_path_json"] = orig_secrets_json_path
    if orig_whylabs_api_key is not None:
        os.environ["WHYLABS_API_KEY"] = orig_whylabs_api_key
    if orig_container_password is not None:
        os.environ["static_secret"] = orig_container_password
    if orig_secrets_path_files is not None:
        os.environ["secrets_path_files"] = orig_secrets_path_files


def test_whylabs_api_key_required(_secrets_file_fixture: IO[bytes]):
    with pytest.raises(ValueError):
        # throws because there is no whylabs_api_key
        ServerConfig()  # pyright: ignore[reportCallIssue]


def test_whylabs_api_key_from_obj(_secrets_file_fixture: IO[bytes]):
    # no error, key directly supplied
    random_api_key = str(uuid.uuid4())
    config = ServerConfig(whylabs_api_key=random_api_key, static_secret=None)
    assert config.whylabs_api_key == random_api_key


def test_whylabs_api_key_from_env(_secrets_file_fixture: IO[bytes]):
    # no error, key from env
    random_api_key = str(uuid.uuid4())
    os.environ["WHYLABS_API_KEY"] = random_api_key
    config = ServerConfig(static_secret=None, disable_client_auth=True)  # pyright: ignore[reportCallIssue]
    assert config.whylabs_api_key == random_api_key


def test_whylabs_api_key_from_secrets_file(_secrets_file_fixture: IO[bytes]):
    random_api_key = str(uuid.uuid4())
    secrets_file = SecretsFile(whylabs_api_key=random_api_key)
    _secrets_file_fixture.write(secrets_file.model_dump_json().encode())
    _secrets_file_fixture.flush()
    _secrets_file_fixture.seek(0)

    # no error, and no explicit whylabs_api_key
    config = ServerConfig(static_secret=None, disable_client_auth=True)  # pyright: ignore[reportCallIssue]
    assert config.whylabs_api_key == random_api_key


def test_container_password_from_obj(_secrets_file_fixture: IO[bytes]):
    # no error, password directly supplied
    random_password = str(uuid.uuid4())
    config = ServerConfig(whylabs_api_key="test", static_secret=random_password)
    assert config.static_secret == random_password


def test_container_password_from_env(_secrets_file_fixture: IO[bytes]):
    # no error, password from env
    random_password = str(uuid.uuid4())
    os.environ["static_secret"] = random_password
    config = ServerConfig(whylabs_api_key="test")
    assert config.static_secret == random_password


def test_container_password_from_secrets_file(_secrets_file_fixture: IO[bytes]):
    random_password = str(uuid.uuid4())
    secrets_file = SecretsFile(static_secret=random_password)
    _secrets_file_fixture.write(secrets_file.model_dump_json().encode())
    _secrets_file_fixture.flush()
    _secrets_file_fixture.seek(0)

    # no error, and no explicit paswsword
    config = ServerConfig(whylabs_api_key="test")
    assert config.static_secret == random_password


def test_none_empty_strings():
    settings = ServerConfig(whylabs_api_key="key", static_secret="password", whylabs_api_cache_endpoint="", aws_sqs_url="")
    assert settings.whylabs_api_cache_endpoint is None
    assert settings.aws_sqs_url is None
