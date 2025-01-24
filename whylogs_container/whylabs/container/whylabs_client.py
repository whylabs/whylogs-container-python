import logging
import time
from dataclasses import dataclass
from functools import cache, lru_cache, partial
from typing import Any, Callable, Dict, List, Optional, Set, TypeVar, cast

import whylabs_client
from tenacity import retry, stop_after_attempt, wait_exponential_jitter
from whylabs_client.api.api_key_api import ApiKeyApi
from whylabs_client.api.organizations_api import OrganizationIdentifier, OrganizationsApi
from whylabs_client.api.policy_api import PolicyApi, PolicyConfigurationListEntry
from whylabs_client.model.add_policy_response import AddPolicyResponse

from whylogs_container.whylabs.container.server_config import get_server_config

_logger = logging.getLogger(__name__)


def _get_epoch_minute():
    current_epoch = int(time.time())
    return current_epoch - (current_epoch % 60)


T = TypeVar("T")

_schema_version = "0.1.0"


@dataclass
class ClientWithFallback:
    cache_client: whylabs_client.ApiClient
    fallback_client: whylabs_client.ApiClient

    def call(self, fn: Callable[[whylabs_client.ApiClient], T]) -> T:
        try:
            return fn(self.cache_client)
        except whylabs_client.ApiException as e:
            _logger.error(f"Error calling cache client. Falling back to direct WhyLabs calls: {e}")

        return fn(self.fallback_client)


class WhyLabsClient:
    def __init__(self):
        self._server_settings = get_server_config()

    @cache
    def _get_client(self, api_key: Optional[str] = None) -> whylabs_client.ApiClient:
        if api_key is None:
            api_key = self._server_settings.whylabs_api_key

        endpoint = self._server_settings.whylabs_api_endpoint
        configuration = whylabs_client.Configuration(host=endpoint)
        configuration.api_key["ApiKeyAuth"] = api_key  # pyright: ignore[reportUnknownMemberType]
        configuration.discard_unknown_keys = True
        return whylabs_client.ApiClient(configuration)

    @cache
    def _get_cache_client(self, api_key: Optional[str] = None) -> Optional[whylabs_client.ApiClient]:
        """
        Get the client that will use the cache endpoint for whylabs, if configured to do so.
        """
        if api_key is None:
            api_key = self._server_settings.whylabs_api_key

        if not self._server_settings.whylabs_api_cache_endpoint:
            return None

        endpoint = self._server_settings.whylabs_api_cache_endpoint
        configuration = whylabs_client.Configuration(host=endpoint)
        configuration.api_key["ApiKeyAuth"] = api_key  # pyright: ignore[reportUnknownMemberType]
        configuration.discard_unknown_keys = True
        return whylabs_client.ApiClient(configuration)

    def _cache_fallback(self, fn: Callable[[whylabs_client.ApiClient], T], api_key: Optional[str] = None) -> T:
        cache_client = self._get_cache_client(api_key)

        if cache_client:
            try:
                return fn(cache_client)
            except Exception as e:
                _logger.error(f"Error calling WhyLabs cache endpoint. Falling back to direct WhyLabs service calls: {e}")

        return fn(self._get_client(api_key))

    @retry(stop=stop_after_attempt(3), wait=wait_exponential_jitter(max=5))
    def _get_policy(self, client: whylabs_client.ApiClient, dataset_id: str) -> Optional[str]:
        policy_api = PolicyApi(client)

        try:
            api_response: AddPolicyResponse = policy_api.get_policy(dataset_id, schema_version=_schema_version)  # pyright: ignore[reportUnknownVariableType,reportUnknownMemberType]
            return str(api_response)  # pyright: ignore[reportUnknownArgumentType]
        except whylabs_client.ApiException as e:
            if e.status == 400:  # pyright: ignore[reportUnknownMemberType]
                return None
            raise e

    def get_policy(self, dataset_id: str) -> Optional[str]:
        """
        Get a single policy for the given dataset id.
        """
        return self._cache_fallback(partial(self._get_policy, dataset_id=dataset_id))

    @retry(stop=stop_after_attempt(3), wait=wait_exponential_jitter(max=5))
    def _get_org_policies(self, client: whylabs_client.ApiClient, org_id: Optional[str] = None) -> List[str]:
        orig_headers = cast(Dict[str, Any], client.default_headers.copy())  # pyright: ignore[reportUnknownMemberType]
        try:
            if org_id:
                # This is the weird way we have to add headers for one off requests. We need to set this
                # special header to tell whylabs to make the reqest on behalf of a child org
                client.default_headers["X-WhyLabs-Organization"] = org_id  # pyright: ignore[reportUnknownMemberType]

            policy_api = PolicyApi(client)

            try:
                api_response = cast(
                    List[PolicyConfigurationListEntry],
                    policy_api.list_organization_policy_configurations(include_policy=True, schema_version=_schema_version),  # pyright: ignore[reportUnknownMemberType]
                )
                return [it.policy for it in api_response]  # pyright: ignore[reportUnknownMemberType]
            except whylabs_client.ApiException as e:
                if e.status == 400:  # pyright: ignore[reportUnknownMemberType]
                    return []
                raise e
        finally:
            client.default_headers = orig_headers

    def get_org_policies(self, org_id: Optional[str] = None) -> List[str]:
        """
        Get all of the policies for each of the datasets in an org.
        :param org_id: The organization id to get the policies for. This is only intended for use with parent/child orgs.
          The normal api key for the container is still used but a special header is set for the child org. When not in
          multi tenancy mode just leave this as None.
        """
        return self._cache_fallback(partial(self._get_org_policies, org_id=org_id))

    @retry(stop=stop_after_attempt(3), wait=wait_exponential_jitter(max=5))
    def _get_managed_orgs_fallback(self, client: whylabs_client.ApiClient) -> List[str]:
        api = OrganizationsApi(client)
        orgs = cast(List[OrganizationIdentifier], api.get_managed_organizations())  # pyright: ignore[reportUnknownMemberType]
        return [org.id for org in orgs]

    @lru_cache(maxsize=1)
    def _get_managed_orgs_cached(self, _current_minute_epoch: int) -> List[str]:
        # Utility wrapper around the api call to cache it with a minute timeout
        return self._cache_fallback(self._get_managed_orgs_fallback)

    def get_managed_orgs(self) -> Set[str]:
        if self._server_settings.whylabs_api_cache_endpoint:
            return set(self._cache_fallback(self._get_managed_orgs_fallback))
        else:
            # Only use the in memory cache if we are not using the cache endpoint, which happens when people aren't using our full
            # kubernetes chart.
            return set(self._get_managed_orgs_cached(_get_epoch_minute()))

    @retry(stop=stop_after_attempt(3), wait=wait_exponential_jitter(max=5))
    def _validate_api_key(self, client: whylabs_client.ApiClient) -> bool:
        api = ApiKeyApi(client)
        try:
            api.validate_api_key()  # pyright: ignore[reportUnknownMemberType]
            return True
        except whylabs_client.ApiException as e:
            if e.status == 403:  # pyright: ignore[reportUnknownMemberType]
                return False
            raise e

    @lru_cache(maxsize=1)
    def _validate_api_key_cached(self, api_key: Optional[str], _current_minute_epoch: int) -> bool:
        return self._cache_fallback(self._validate_api_key, api_key)

    def validate_api_key(self, api_key: Optional[str] = None) -> bool:
        """
        Validate the API key is valid and has the correct permissions.
        """
        if self._server_settings.whylabs_api_cache_endpoint:
            return self._cache_fallback(self._validate_api_key, api_key)
        else:
            # Only use the in memory cache if we are not using the cache endpoint, which happens when people aren't using our full
            # kubernetes chart.
            return self._validate_api_key_cached(api_key, _get_epoch_minute())


__instance: Optional[WhyLabsClient] = None


def get_whylabs_client() -> WhyLabsClient:
    global __instance
    if __instance is None:
        __instance = WhyLabsClient()
    return __instance


def reset_whylabs_client():
    global __instance
    __instance = None
