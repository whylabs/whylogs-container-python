from typing import Any, cast


def _hashable(it: Any):
    if isinstance(it, list):
        return HashableList([_hashable(i) for i in it])  # pyright: ignore[reportUnknownVariableType]
    if isinstance(it, dict):
        return HashableDict({k: _hashable(v) for k, v in it.items()})  # pyright: ignore[reportUnknownVariableType]
    return it


class HashableDict(dict[str, Any]):
    def __key(self):
        return tuple((k, _hashable(self[k])) for k in sorted(self))

    def __hash__(self):  # pyright: ignore[reportIncompatibleVariableOverride]
        return hash(self.__key())

    def __eq__(self, other: Any):
        if not isinstance(other, dict):
            return False

        o = cast(Any, other)
        return self.__key() == o.__key()

    @staticmethod
    def from_dict(value: dict[str, Any]) -> "HashableDict":
        return HashableDict({k: HashableDict.from_dict(v) if isinstance(v, dict) else v for k, v in value.items()})  # pyright: ignore[reportUnknownArgumentType]


class HashableList(list[Any]):
    def __key(self):
        return tuple(self)

    def __hash__(self):  # pyright: ignore[reportIncompatibleVariableOverride]
        return hash(self.__key())

    def __eq__(self, other: Any):
        if not isinstance(other, list):
            return False

        o = cast(Any, other)
        return self.__key() == o.__key()

    @staticmethod
    def from_list(value: list[Any]) -> "HashableList":
        return HashableList(
            [HashableList.from_list(v) if isinstance(v, list) else HashableDict.from_dict(v) if isinstance(v, dict) else v for v in value]  # pyright: ignore[reportUnknownArgumentType]
        )
