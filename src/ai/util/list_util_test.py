from .list_util import get_like_items, type_batched_items
from typing import List


def test_get_like_items_hp() -> None:
    l = [1, 2, 3, 4, 5, "a", 6, 7, 8]
    (batch, batch_type, next) = get_like_items(l)

    assert batch == [1, 2, 3, 4, 5]
    assert batch_type == type(1)
    assert next == ["a", 6, 7, 8]


def test_get_like_items_single_item() -> None:
    l = [1]
    (batch, batch_type, next) = get_like_items(l)

    assert batch == [1]
    assert batch_type == type(1)
    assert next == []


def test_get_like_items_empty() -> None:
    l: List[int] = []
    (batch, batch_type, next) = get_like_items(l)

    assert batch == []
    assert batch_type == type(None)
    assert next == []


def test_get_like_items_single_type() -> None:
    l: List[int] = [1, 2, 3, 4, 5, 6, 7, 8]
    (batch, batch_type, next) = get_like_items(l)

    assert batch == [1, 2, 3, 4, 5, 6, 7, 8]
    assert batch_type == type(1)
    assert next == []


def test_iter() -> None:
    l = [1, 2, 3, 4, 5, "a", 6, 7, 8]
    i = 0
    for (batch, batch_type) in type_batched_items(l):
        if i == 0:
            assert batch == [1, 2, 3, 4, 5]
            assert batch_type == type(1)
        elif i == 1:
            assert batch == ["a"]
            assert batch_type == type("a")
        elif i == 2:
            assert batch == [6, 7, 8]
            assert batch_type == type(1)
        else:
            raise Exception("Shouldn't happen")
        i += 1


def test_get_like_items_until_empty() -> None:
    l = [1, "a", 2.0]

    (batch, batch_type, next) = get_like_items(l)
    assert batch == [1]
    assert batch_type == type(1)
    assert next == ["a", 2.0]

    (batch, batch_type, next) = get_like_items(next)
    assert batch == ["a"]
    assert batch_type == type("a")
    assert next == [2.0]

    (batch, batch_type, next) = get_like_items(next)
    assert batch == [2.0]
    assert batch_type == type(1.0)
    assert next == []
