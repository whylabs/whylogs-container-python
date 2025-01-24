import logging
from dataclasses import dataclass
from functools import cache
from typing import Optional

from fastapi import Depends, HTTPException
from fastapi.security.api_key import APIKeyHeader

from whylogs_container.whylabs.container.dependencies.api_key_util import parse_api_key
from whylogs_container.whylabs.container.dependencies.header_values import HeaderNames
from whylogs_container.whylabs.container.server_config import get_server_config
from whylogs_container.whylabs.container.whylabs_client import get_whylabs_client

_logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class WhyLabsApiKey:
    api_key: str
    id: str
    org_id: str

    @staticmethod
    def parse(api_key: str) -> "WhyLabsApiKey":
        try:
            parsed = parse_api_key(api_key)
        except Exception as e:
            raise HTTPException(status_code=403, detail=f"Invalid API key format. {str(e)}")

        return WhyLabsApiKey(api_key=api_key, id=parsed.id, org_id=parsed.org_id)


@cache
def api_key_dependency():
    settings = get_server_config()
    client = get_whylabs_client()

    async def wrapper(
        x_whylabs_api_key: Optional[str] = Depends(APIKeyHeader(name=HeaderNames.whylabs_api_key_header.value, auto_error=False)),
    ) -> WhyLabsApiKey:
        if settings.auth_disabled():
            # If auth is disabled then disregard the client api token and just use the one configured at the container level
            # for everything.
            _logger.info("Auth is disabled, skipping request validation.")
            return WhyLabsApiKey.parse(settings.whylabs_api_key)

        if not x_whylabs_api_key:
            raise HTTPException(
                status_code=403,
                detail=f"{HeaderNames.whylabs_api_key_header.value} header is required",
            )

        if settings.static_secret:
            if x_whylabs_api_key != settings.static_secret:
                raise HTTPException(
                    status_code=403,
                    detail="Secret didn't match. This container is configured with static credentials, not remote api key auth.",
                )

            # If we're using static secrets then just use the container level api key for everything
            return WhyLabsApiKey.parse(settings.whylabs_api_key)

        api_key = WhyLabsApiKey.parse(x_whylabs_api_key)

        # Validate that the API key is valid
        if not client.validate_api_key(api_key.api_key):
            raise HTTPException(
                status_code=403,
                detail=f"API key with id {api_key.id} failed authentication",
            )

        # For multitentant mode, we need to ensure that the org_id is managed by the parent org as well
        if settings.tenancy_mode == "MULTI":
            orgs = client.get_managed_orgs()

            if settings.enforce_tenancy_mode and api_key.org_id not in orgs:
                container_key = WhyLabsApiKey.parse(settings.whylabs_api_key)
                raise HTTPException(
                    status_code=403,
                    detail=f"Organization with id {api_key.id} and org {api_key.org_id} is not managed by the parent "
                    f"organization specified in the container level api key {container_key.org_id}. This error is happening"
                    " because the container is running in multi tenant mode which requires all request API keys to come from"
                    " organizations managed by the parent organization.",
                )
        else:
            if api_key.org_id != settings.org_id:
                raise HTTPException(
                    status_code=403,
                    detail=f"Organization with id {api_key.org_id} is not the same as the organization id {settings.org_id} "
                    "configured in the container. This error is happening because the container is running in single tenant mode"
                    " which requires all request API keys to come from the same organization.",
                )

        return api_key

    return wrapper
