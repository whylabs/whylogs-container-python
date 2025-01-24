from dataclasses import dataclass
from functools import cache
from typing import Tuple


def _strict_split(text: str, delimiter: str) -> Tuple[str, str]:
    parts = text.split(delimiter, 1)
    if len(parts) < 2:
        raise ValueError(f"Delimiter '{delimiter}' not found in text")
    return parts[0], parts[1]


@dataclass
class ParsedApiKey:
    org_id: str
    id: str


@cache
def parse_api_key(api_key: str) -> ParsedApiKey:
    org_id = _strict_split(api_key, ":")[1]
    id = _strict_split(api_key, ".")[0]

    if not org_id:
        raise Exception("Invalid API key format. Couldn't parse the organization id from the key.")

    if not id:
        raise Exception("Invalid API key format. Couldn't parse the id from the key.")

    return ParsedApiKey(org_id=org_id, id=id)
