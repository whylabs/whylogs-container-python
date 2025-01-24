from typing import Any, Coroutine


async def bool_not(coro: Coroutine[Any, Any, bool]):
    result = await coro
    return not result
