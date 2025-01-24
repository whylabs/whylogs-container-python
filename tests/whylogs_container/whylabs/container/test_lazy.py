from whylogs_container.whylabs.container.lazy import LazyInit


def test_lazy_init():
    i = 0

    def init():
        nonlocal i
        i += 1
        return "test"

    lazy = LazyInit(init)

    assert lazy.value == "test"
    assert i == 1
    assert lazy.value == "test"
    assert i == 1
