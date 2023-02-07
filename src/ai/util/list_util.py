from itertools import takewhile
from typing import Any, Dict, List, Tuple, Type, Union


def get_like_items(items: List[Any]) -> Tuple[List[Any], Type, List[Any]]:
    if not items:
        return ([], type(None), items)

    item_type = type(items[0])
    matches = list(takewhile(lambda item: isinstance(item, item_type), items))
    return (matches, item_type, items[len(matches) :])
